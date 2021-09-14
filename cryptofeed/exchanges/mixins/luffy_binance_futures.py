'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import hashlib
import hmac
import logging
import time
from time import sleep
from decimal import Decimal

import pandas as pd
import requests
from yapic import json

from cryptofeed.defines import BINANCE_FUTURES, BINANCE_DELIVERY, BID, ASK, BUY, SELL, CANCELLED, COINBASE, FILLED, LIMIT, MARKET, OPEN, PARTIAL, PENDING, REJECTED, EXPIRED, ORDERS
from cryptofeed.rest.api import API, request_retry
from cryptofeed.rest.luffy_api_feed import APIFeed
from cryptofeed.standards import symbol_exchange_to_std, symbol_std_to_exchange, timestamp_normalize

REQUEST_LIMIT = 1000
RATE_LIMIT_SLEEP = 3
LOG = logging.getLogger('rest')


# binance common class for signature and request... @logan
class Binance(APIFeed):
    def _get(self, endpoint, retry, retry_wait):
        @request_retry(self.ID, retry, retry_wait)
        def helper():
            r = requests.get(f"{self.api}{endpoint}")
            self._handle_error(r, LOG)
            return r.json()

        return helper()

    def _nonce(self):
        return str(int(round(time.time() * 1000)))


    def _generate_signature(self, body=json.dumps({})):
        query_string = '&'.join(["{}={}".format(key, value) for key, value in body.items()])
        m = hmac.new(self.config.key_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)

        return m.hexdigest()

    # ref from https://github.com/sammchardy/python-binance @logan
    # def _request(self, method, uri, signed, force_params=False, **kwargs):
    def _request(self, method: str, endpoint: str, auth: bool = False, body=None, retry=None, retry_wait=0):

        @request_retry(self.ID, retry, retry_wait)
        def helper(verb, api, endpoint, body, auth):
            header = None
            params = None
            args = {}

            if body and isinstance(body, dict):
                args = body

            if auth:
                args['timestamp'] = int(time.time() * 1000)
                args['signature'] = self._generate_signature(args)
                # params = '&'.join('%s=%s' % (key, value) for key, value in args.items())
                # params = '&'.join('%s=%s' % (data[0], data[1]) for data in args)
                # header = json.dumps({"X-MBX-APIKEY": self.config.key_id,
                #                     "content-type": "application/json"})
                header = {"X-MBX-APIKEY": self.config.key_id,
                          "content-type": "application/json"}
            
            params = '&'.join('%s=%s' % (key, value) for key, value in args.items())

            url = f'{api}{endpoint}?{params}' 

            if method == 'GET':
                return requests.get(f'{api}{endpoint}?{params}', headers=header)
            elif method == 'POST':
                return requests.post(f'{api}{endpoint}?{params}', json=body, headers=header)
            elif method == 'DELETE':
                return requests.delete(f'{api}{endpoint}?{params}', headers=header)

        return helper(method, self.api, endpoint, body, auth)

    def _trade_normalization(self, symbol: str, trade: list) -> dict:
        ret = {
            'timestamp': timestamp_normalize(self.ID, trade['T']),
            'symbol': symbol_exchange_to_std(symbol),
            'id': trade['a'],
            'feed': self.ID,
            'side': BUY if trade['m'] is True else SELL,
            'amount': abs(float(trade['q'])),
            'price': float(trade['p']),
        }

        return ret

    @staticmethod
    def _order_status(data: dict):
        if 'status' not in data:
            raise UnexpectedMessage(f"Message from exchange: {data}")
        status = data['status']
        if data['status'] == 'FILLED':
            status = FILLED
        elif data['status'] == 'NEW':
            status = OPEN
        elif data['status'] == 'EXPIRED':
            status = EXPIRED
        elif data['status'] == 'REJECTED':
            status = REJECTED
        elif data['status'] == 'PARTIALLY_FILLED':
            status = PARTIAL
        # elif data['status'] == 'PENDING_CANCEL':
        #     status = 'PENDING_CANCEL'
        elif data['status'] == 'CANCELLED':
            status = CANCELLED

        # if 'price' not in data:
        #     price = Decimal(data['executed_value']) / Decimal(data['filled_size'])
        # else:
        price = Decimal(data['avgPrice'])

        return {
            'order_id': data['orderId'],
            'symbol': data['symbol'],
            'side': BUY if data['side'] == 'BUY' else SELL,
            'order_type': LIMIT if data['type'] == 'LIMIT' else MARKET,
            'price': price,
            'total': Decimal(data['origQty']),
            'executed': Decimal(data['executedQty']),
            'pending': Decimal(data['origQty']) - Decimal(data['executedQty']),
            # 'timestamp': data['updateTime'].timestamp() if 'updateTime' in data else data['time'].timestamp(),
            'timestamp': data['updateTime'] if 'updateTime' in data else data['time'],
            'order_status': status
        }

    # message handler
    async def message_handler(self, **channels):
        msg = json.loads(msg, parse_float=Decimal)

        # Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        # streamName is of format <symbol>@<channel>
        # pair, _ = msg['stream'].split('@', 1)
        # msg = msg['data']
        # pair = pair.upper()

        for channel, symbols in channels.items():
            if channel == ORDERS:
                for symbol in symbols:
                    # msg = self.orders(symbol=symbol)
                    # self.callbacks[ORDERS]
                    # await self._feed_orders(msg, timestamp)
                    await self._feed_orders(symbol)
            else:
                LOG.warning("%s: Unexpected channel received: %s", self.ID, channel)


class BinanceDelivery(Binance):
    ID = BINANCE_DELIVERY
    api = "https://dapi.binance.com/dapi/v1/"

    # modified by @logan
    def _get_trades_hist(self, symbol, start_date, end_date, retry, retry_wait):
        start = None
        end = None
        body = None

        if start_date:
            if not end_date:
                end_date = pd.Timestamp.utcnow()
            start = API._timestamp(start_date)
            end = API._timestamp(end_date) - pd.Timedelta(nanoseconds=1)

            start = int(start.timestamp() * 1000)
            end = int(end.timestamp() * 1000)

        if start and end:
            body = {
            'symbol': symbol,
            'limit': REQUEST_LIMIT,
            'startTime': start,
            'endTime': end
            }
        else:
           body = {
            'symbol': symbol
           } 

        while True:
            r = self._request('GET', "/aggTrades", body=body)

            if r.status_code == 429:
                sleep(int(r.headers['Retry-After']))
                continue
            elif r.status_code == 500:
                LOG.warning("%s: 500 for URL %s - %s", self.ID, r.url, r.text)
                sleep(retry_wait)
                continue
            elif r.status_code != 200:
                self._handle_error(r, LOG)
            else:
                sleep(RATE_LIMIT_SLEEP)

            data = r.json()
            test = data[-1]['T']
            if data == []:
                LOG.warning("%s: No data for range %d - %d", self.ID, start, end)
            else:
                if data[-1]['T'] == start:
                    LOG.warning("%s: number of trades exceeds exchange time window, some data will not be retrieved for time %d", self.ID, start)
                    start += 1
                else:
                    start = data[-1]['T']

            data = list(map(lambda x: self._trade_normalization(symbol, x), data))
            yield data

            if len(data) < REQUEST_LIMIT:
                break

    def trades(self, symbol: str, start=None, end=None, retry=None, retry_wait=10):
        symbol = symbol_std_to_exchange(symbol, self.ID)
        for data in self._get_trades_hist(symbol, start, end, retry, retry_wait):
            yield data

    def balances(self):
        resp = self._request('GET', "/balance", auth=True)
        self._handle_error(resp, LOG)

        # result = json.loads(resp.text, parse_float=Decimal)

        return {
            entry['asset']: {
                'total': Decimal(entry['balance']),
                'available': Decimal(entry['availableBalance'])
            }
            for entry in json.loads(resp.text, parse_float=Decimal)
        }

    def orders(self, symbol: str):
        endpoint = "/allOrders"
        symbol = symbol_std_to_exchange(symbol, self.ID)
        data = self._request("GET", endpoint, auth=True, body={'symbol': symbol})
        data = json.loads(data.text, parse_float=Decimal)
        return [Binance._order_status(order) for order in data]

    def order_status(self, order_id: str):
        endpoint = f"/allOrders/{order_id}"
        order = self._request("GET", endpoint, auth=True)
        order = json.loads(order.text, parse_float=Decimal)
        return Coinbase._order_status(order)

    # feed handelers @logan
    # async def _feed_orders(self, msg: dict, timestamp: float):
    async def _feed_orders(self, symbol: str):
        """
        order msg example

        {
            "orderId": 6000942672,
            "symbol": "BTCUSD_PERP",
            "pair": "BTCUSD",
            "status": "FILLED",
            "clientOrderId": "web_8kNuOXyzjAEpSsy9TTbB",
            "price": "0",
            "avgPrice": "45673.2",
            "origQty": "10",
            "executedQty": "10",
            "cumBase": "0.02189467",
            "timeInForce": "GTC",
            "type": "MARKET",
            "reduceOnly": false,
            "closePosition": false,
            "side": "BUY",
            "positionSide": "BOTH",
            "stopPrice": "0",
            "workingType": "CONTRACT_PRICE",
            "priceProtect": false,
            "origType": "MARKET",
            "time": 1614328188507,
            "updateTime": 1614328192285
        }
        """

        msg = self.orders(symbol=symbol)

        for order in msg:
            await self.callback(ORDERS, feed=self.ID,
                                symbol=symbol_exchange_to_std(data['symbol']),
                                order_id=data['orderId'],
                                order_alias=data['clientOrderId'],
                                status=data['status'], 
                                side=BUY if data['side'] == 'BUY' else SELL,
                                price=Decimal(data['avgPrice']),
                                qty=data['origQty'],
                                exec_qty=Decimal(data['leavesQty']),
                                type=data['origType'],
                                time_in_force=data['timeInForce'],
                                timestamp=data['updateTime'])
                                # receipt_timestamp=timestamp)


class BinanceFutures(Binance):
    ID = BINANCE_FUTURES
    api = "https://fapi.binance.com/fapi/v1/"

    def _get_trades_hist(self, symbol, start_date, end_date, retry, retry_wait):
        start = None
        end = None

        if start_date:
            if not end_date:
                end_date = pd.Timestamp.utcnow()
            start = API._timestamp(start_date)
            end = API._timestamp(end_date) - pd.Timedelta(nanoseconds=1)

            start = int(start.timestamp() * 1000)
            end = int(end.timestamp() * 1000)

        if start and end:
            body = {
            'symbol': symbol,
            'limit': REQUEST_LIMIT,
            'startTime': start,
            'endTime': end
            }
        else:
           body = {
            'symbol': symbol
           } 

        while True:
            r = self._request('GET', "/aggTrades", body=body)

            if r.status_code == 429:
                sleep(int(r.headers['Retry-After']))
                continue
            elif r.status_code == 500:
                LOG.warning("%s: 500 for URL %s - %s", self.ID, r.url, r.text)
                sleep(retry_wait)
                continue
            elif r.status_code != 200:
                self._handle_error(r, LOG)
            else:
                sleep(RATE_LIMIT_SLEEP)

            data = r.json()
            if data == []:
                LOG.warning("%s: No data for range %d - %d", self.ID, start, end)
            else:
                if data[-1]['T'] == start:
                    LOG.warning(
                        "%s: number of trades exceeds exchange time window, some data will not be retrieved for time %d",
                        self.ID, start)
                    start += 1
                else:
                    start = data[-1]['T']

            data = list(map(lambda x: self._trade_normalization(symbol, x), data))
            yield data

            if len(data) < REQUEST_LIMIT:
                break

    def trades(self, symbol: str, start=None, end=None, retry=None, retry_wait=10):
        symbol = symbol_std_to_exchange(symbol, self.ID)
        for data in self._get_trades_hist(symbol, start, end, retry, retry_wait):
            yield data
