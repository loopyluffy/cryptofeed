'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging
from typing import List, Tuple, Callable, Dict

from yapic import json

from cryptofeed.connection import AsyncConnection, HTTPPoll
from cryptofeed.defines import BALANCES, BINANCE_FUTURES, BUY, FUNDING, LIMIT, LIQUIDATIONS, MARKET, OPEN_INTEREST, ORDER_INFO, POSITIONS, SELL, ACCOUNT_CONFIG, STOP_MARKET, STOP_LIMIT
from cryptofeed.exchanges.binance import Binance
from cryptofeed.exchanges.mixins.binance_rest import BinanceFuturesRestMixin
from cryptofeed.types import OpenInterest, OrderInfo, Balance, LoopyBalance, LoopyPosition

LOG = logging.getLogger('feedhandler')


class BinanceFutures(Binance, BinanceFuturesRestMixin):
    id = BINANCE_FUTURES
    symbol_endpoint = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    listen_key_endpoint = 'listenKey'
    valid_depths = [5, 10, 20, 50, 100, 500, 1000]
    valid_depth_intervals = {'100ms', '250ms', '500ms'}
    websocket_channels = {
        **Binance.websocket_channels,
        FUNDING: 'markPrice',
        OPEN_INTEREST: 'open_interest',
        LIQUIDATIONS: 'forceOrder',
        POSITIONS: POSITIONS

        # authenticated channel test @logan
        # deprecated for update of @mboscon
        # BALANCES: 'JEI6zo112RvYorpuhGZ16hhkoC7HkThoPqromIUwRlMGerraTERNmDmiowHrSbxA'
        # 'userData': 'userData'
    }

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        base, info = super()._parse_symbol_data(data)
        add = {}
        for symbol, orig in base.items():
            if "_" in orig:
                continue
            add[f"{symbol.replace('PERP', 'PINDEX')}"] = f"p{orig}"
        base.update(add)
        return base, info

    def __init__(self, open_interest_interval=1.0, **kwargs):
        """
        open_interest_interval: flaot
            time in seconds between open_interest polls
        """
        super().__init__(**kwargs)
        # overwrite values previously set by the super class Binance
        self.ws_endpoint = 'wss://fstream.binance.com'
        self.rest_endpoint = 'https://fapi.binance.com/fapi/v1'
        self.address = self._address()
        self.ws_defaults['compression'] = None

        self.open_interest_interval = open_interest_interval

    def _check_update_id(self, pair: str, msg: dict) -> bool:
        if self._l2_book[pair].delta is None and msg['u'] < self.last_update_id[pair]:
            return True
        elif msg['U'] <= self.last_update_id[pair] <= msg['u']:
            self.last_update_id[pair] = msg['u']
            return False
        elif self.last_update_id[pair] == msg['pu']:
            self.last_update_id[pair] = msg['u']
            return False
        else:
            self._reset()
            LOG.warning("%s: Missing book update detected, resetting book", self.id)
            return True

    async def _open_interest(self, msg: dict, timestamp: float):
        """
        {
            "openInterest": "10659.509",
            "symbol": "BTCUSDT",
            "time": 1589437530011   // Transaction time
        }
        """
        pair = msg['symbol']
        oi = msg['openInterest']
        if oi != self._open_interest_cache.get(pair, None):
            o = OpenInterest(
                self.id,
                self.exchange_symbol_to_std_symbol(pair),
                Decimal(oi),
                self.timestamp_normalize(msg['time']),
                raw=msg
            )
            await self.callback(OPEN_INTEREST, o, timestamp)
            self._open_interest_cache[pair] = oi

    # ----------------------------------------------------------------------------------------
    # start user data stream @logan
    # deprecated for update of @mboscon
    """
    async def _start_user_data_stream(self):
        from cryptofeed.defines import POST

        data = await self._request(POST, 'listenKey', auth=True)
        self.websocket_channels['userData'] = data['listenKey'] 
        # return data['listenKey']

    async def subscribe(self, conn: AsyncConnection):
        await super().subscribe(conn)

        if self.subscription is not None:
            for channel in self.subscription:
                # chan = self.std_channel_to_exchange(channel)
                if channel == 'userData':
                    await self._start_user_data_stream()    
    """
    # ----------------------------------------------------------------------------------------

    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        ret = []
        if self.address:
            ret = super().connect()
        PollCls = HTTPPoll
        for chan in set(self.subscription):
            if chan == 'open_interest':
                addrs = [f"{self.rest_endpoint}/openInterest?symbol={pair}" for pair in self.subscription[chan]]
                ret.append((PollCls(addrs, self.id, delay=60.0, sleep=self.open_interest_interval, proxy=self.http_proxy), self.subscribe, self.message_handler, self.authenticate))

        # ----------------------------------------------------------------------------------------
        # connect another socket for user data stream... @logan
        # deprecated for update of @mboscon
        """
        from cryptofeed.connection import WSAsyncConn # AsyncConnection, HTTPAsyncConn, 

        if self.subscription is not None:
            for channel in self.subscription:
                # chan = self.std_channel_to_exchange(channel)
                if channel == 'userData':
                    # address = self.ws_endpoint + '/ws/rUzZ3D0gsNR8Yl5QbI1Opk2z8oXIjIHCoPoEgFYr7fI5VZievgsrYdoHOUAEoTqQ'
                    # address = f"{self.ws_endpoint}/ws/JEI6zo112RvYorpuhGZ16hhkoC7HkThoPqromIUwRlMGerraTERNmDmiowHrSbxA"
                    # task = create_task(self._start_user_data_stream())
                    # data = await task
                    address = self.ws_endpoint + '/ws/' + self.websocket_channels['userData']
                    ret.append((WSAsyncConn(address, self.id, **self.ws_defaults), self.subscribe, self.message_handler, self.authenticate))
        """
        # ----------------------------------------------------------------------------------------

        return ret

    # handle another user data stream... @logan
    async def _account_config_update(self, msg: dict, timestamp: float):
        """
        {
            "e":"ACCOUNT_CONFIG_UPDATE",       // Event Type
            "E":1611646737479,                 // Event Time
            "T":1611646737476,                 // Transaction Time
            "ac":{                              
            "s":"BTCUSDT",                     // symbol
            "l":25                             // leverage

            }
        }  

        or

        {
            "e":"ACCOUNT_CONFIG_UPDATE",       // Event Type
            "E":1611646737479,                 // Event Time
            "T":1611646737476,                 // Transaction Time
            "ai":{                             // User's Account Configuration
            "j":true                           // Multi-Assets Mode
            }
        }  
        """
        
        await self.callback(ACCOUNT_CONFIG, 
                            {'timestamp': self.timestamp_normalize(msg['E']),
                            'receipt_timestamp': self.timestamp_normalize(msg['E']),
                            'symbol': msg['ac']['s'] if 'ac' in msg else None,
                            'leverage': msg['ac']['l'] if 'ac' in msg else None,
                            'multi_asset_mode': msg['ai']['j'] if 'ai' in msg else None},
                            timestamp)

        # await self.callback(ACCOUNT_CONFIG, 
        #                     timestamp=self.timestamp_normalize(msg['E']),
        #                     receipt_timestamp=self.timestamp_normalize(msg['E']),
        #                     symbol=msg['ac']['s'] if 'ac' in msg else None,
        #                     leverage=msg['ac']['l'] if 'ac' in msg else None,
        #                     multi_asset_mode=msg['ai']['j'] if 'ai' in msg else None)


    async def _account_update(self, msg: dict, timestamp: float):
        """
        {
        "e": "ACCOUNT_UPDATE",                // Event Type
        "E": 1564745798939,                   // Event Time
        "T": 1564745798938 ,                  // Transaction
        "a":                                  // Update Data
            {
            "m":"ORDER",                      // Event reason type
            "B":[                             // Balances
                {
                "a":"USDT",                   // Asset
                "wb":"122624.12345678",       // Wallet Balance
                "cw":"100.12345678",          // Cross Wallet Balance
                "bc":"50.12345678"            // Balance Change except PnL and Commission
                },
                {
                "a":"BUSD",
                "wb":"1.00000000",
                "cw":"0.00000000",
                "bc":"-49.12345678"
                }
            ],
            "P":[
                {
                "s":"BTCUSDT",            // Symbol
                "pa":"0",                 // Position Amount
                "ep":"0.00000",            // Entry Price
                "cr":"200",               // (Pre-fee) Accumulated Realized
                "up":"0",                     // Unrealized PnL
                "mt":"isolated",              // Margin Type
                "iw":"0.00000000",            // Isolated Wallet (if isolated position)
                "ps":"BOTH"                   // Position Side
                }，
                {
                    "s":"BTCUSDT",
                    "pa":"20",
                    "ep":"6563.66500",
                    "cr":"0",
                    "up":"2850.21200",
                    "mt":"isolated",
                    "iw":"13200.70726908",
                    "ps":"LONG"
                },
                {
                    "s":"BTCUSDT",
                    "pa":"-10",
                    "ep":"6563.86000",
                    "cr":"-45.04000000",
                    "up":"-1423.15600",
                    "mt":"isolated",
                    "iw":"6570.42511771",
                    "ps":"SHORT"
                }
            ]
            }
        }
        """
        # for balance in msg['a']['B']:
        #     await self.callback(BALANCES,
        #                         feed=self.id,
        #                         symbol=balance['a'],
        #                         timestamp=self.timestamp_normalize(msg['E']),
        #                         receipt_timestamp=timestamp,
        #                         wallet_balance=Decimal(balance['wb']))
        # for position in msg['a']['P']:
        #     await self.callback(POSITIONS,
        #                         feed=self.id,
        #                         symbol=self.exchange_symbol_to_std_symbol(position['s']),
        #                         timestamp=self.timestamp_normalize(msg['E']),
        #                         receipt_timestamp=timestamp,
        #                         position_amount=Decimal(position['pa']),
        #                         entry_price=Decimal(position['ep']),
        #                         unrealised_pnl=Decimal(position['up']))

        # to sync callback parameters @logan
        for balance in msg['a']['B']:

            bal = LoopyBalance(
                exchange=self.id,
                currency=balance['a'],
                balance=Decimal(balance['wb']),
                cw_balance=Decimal(balance['cw']),
                # how can get reserved balance...? @logan
                reserved=Decimal(balance['wb']) - Decimal(0),
                changed=Decimal(balance['bc']),
                timestamp=self.timestamp_normalize(msg['E']),
                raw=balance)

            await self.callback(BALANCES, bal, timestamp)

        for position in msg['a']['P']:

            pos = LoopyPosition(
                exchange=self.id,
                symbol=self.exchange_symbol_to_std_symbol(position['s']),
                margin_type=position['mt'],
                side='long' if Decimal(position['pa']) > 0 else 'short' if Decimal(position['pa']) < 0 else position['ps'],
                # side=position['ps'],
                entry_price=Decimal(position['ep']),
                amount=Decimal(position['pa']),
                unrealised_pnl=Decimal(position['up']),
                cum_pnl=Decimal(position['cr']),
                timestamp=self.timestamp_normalize(msg['E']),
                raw=balance)

            await self.callback(POSITIONS, pos, timestamp)

    async def _order_update(self, msg: dict, timestamp: float):
        """
        {
            "e":"ORDER_TRADE_UPDATE",     // Event Type
            "E":1568879465651,            // Event Time
            "T":1568879465650,            // Transaction Time
            "o":
            {
                "s":"BTCUSDT",              // Symbol
                "c":"TEST",                 // Client Order Id
                // special client order id:
                // starts with "autoclose-": liquidation order
                // "adl_autoclose": ADL auto close order
                "S":"SELL",                 // Side
                "o":"TRAILING_STOP_MARKET", // Order Type
                "f":"GTC",                  // Time in Force
                "q":"0.001",                // Original Quantity
                "p":"0",                    // Original Price
                "ap":"0",                   // Average Price
                "sp":"7103.04",             // Stop Price. Please ignore with TRAILING_STOP_MARKET order
                "x":"NEW",                  // Execution Type
                "X":"NEW",                  // Order Status
                "i":8886774,                // Order Id
                "l":"0",                    // Order Last Filled Quantity
                "z":"0",                    // Order Filled Accumulated Quantity
                "L":"0",                    // Last Filled Price
                "N":"USDT",             // Commission Asset, will not push if no commission
                "n":"0",                // Commission, will not push if no commission
                "T":1568879465651,          // Order Trade Time
                "t":0,                      // Trade Id
                "b":"0",                    // Bids Notional
                "a":"9.91",                 // Ask Notional
                "m":false,                  // Is this trade the maker side?
                "R":false,                  // Is this reduce only
                "wt":"CONTRACT_PRICE",      // Stop Price Working Type
                "ot":"TRAILING_STOP_MARKET",    // Original Order Type
                "ps":"LONG",                        // Position Side
                "cp":false,                     // If Close-All, pushed with conditional order
                "AP":"7476.89",             // Activation Price, only puhed with TRAILING_STOP_MARKET order
                "cr":"5.0",                 // Callback Rate, only puhed with TRAILING_STOP_MARKET order
                "rp":"0"                            // Realized Profit of the trade
            }
        }
        """
        oi = OrderInfo(
            exchange=self.id,
            symbol=self.exchange_symbol_to_std_symbol(msg['o']['s']),
            # in binance order id is number @logan
            id=str(msg['o']['i']),
            # id=msg['o']['i'],
            side=BUY if msg['o']['S'].lower() == 'buy' else SELL,
            status=msg['o']['X'],  # order status is not excution type @logan
            type=LIMIT if msg['o']['o'].lower() == 'limit' else MARKET if msg['o']['o'].lower() == 'market' else None,
            # if never partially filled, price is original price... @logan
            price=Decimal(msg['o']['ap']) if not Decimal.is_zero(Decimal(msg['o']['ap'])) else Decimal(msg['o']['p']),
            # price=Decimal(msg['o']['ap']) if not Decimal.is_zero(Decimal(msg['o']['ap'])) else None,
            amount=Decimal(msg['o']['q']),
            remaining=Decimal(msg['o']['q']) - Decimal(msg['o']['z']),
            timestamp=self.timestamp_normalize(msg['E']),
            raw=msg
        )
        await self.callback(ORDER_INFO, oi, timestamp)

    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        # Handle REST endpoint messages first
        if 'openInterest' in msg:
            return await self._open_interest(msg, timestamp)

        # Handle account updates from User Data Stream
        if self.requires_authentication:
            msg_type = msg.get('e')
            if msg_type == 'ACCOUNT_UPDATE':
                await self._account_update(msg, timestamp)
            elif msg_type == 'ORDER_TRADE_UPDATE':
                await self._order_update(msg, timestamp)
            # handle another user data stream... @logan
            # elif msg_type == 'ACCOUNT_CONFIG_UPDATE':
            #     await self._account_config_update(msg, timestamp)
            return

        # Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        # streamName is of format <symbol>@<channel>
        pair, _ = msg['stream'].split('@', 1)
        msg = msg['data']

        pair = pair.upper()

        msg_type = msg.get('e')
        if msg_type == 'bookTicker':
            await self._ticker(msg, timestamp)
        elif msg_type == 'depthUpdate':
            await self._book(msg, pair, timestamp)
        elif msg_type == 'aggTrade':
            await self._trade(msg, timestamp)
        elif msg_type == 'forceOrder':
            await self._liquidations(msg, timestamp)
        elif msg_type == 'markPriceUpdate':
            await self._funding(msg, timestamp)
        elif msg['e'] == 'kline':
            await self._candle(msg, timestamp)
        else:
            LOG.warning("%s: Unexpected message received: %s", self.id, msg)

    # for stop loss order... @logan
    async def place_order(self, symbol: str, side: str, order_type: str, amount: Decimal, price=None, stop_price=None, closePosition=False,  time_in_force=None, test=False):
        if (order_type == MARKET or order_type == STOP_MARKET) and price:
            raise ValueError('Cannot specify price on a market order')
        if order_type == LIMIT:
            if not price:
                raise ValueError('Must specify price on a limit order')
            if not time_in_force:
                raise ValueError('Must specify time in force on a limit order') 
        if order_type == STOP_MARKET:
            if not stop_price:
                raise ValueError('Must specify stop_price on a stop market order')
        if order_type == STOP_LIMIT:
            if not price:
                raise ValueError('Must specify price on a stop order')
            if not stop_price:
                raise ValueError('Must specify stop_price on a stop order')
                
            
        ot = self.normalize_order_options(order_type)
        sym = self.std_symbol_to_exchange_symbol(symbol)
        parameters = {
            'symbol': sym,
            'side': 'BUY' if side is BUY else 'SELL',
            'type': ot,
            'quantity': str(amount),
        }

        if price:
            if order_type == STOP_MARKET:
                parameters['stopPrice'] = str(price)
                parameters['closePosition'] = closePosition
            else:
                parameters['price'] = str(price)
        if time_in_force:
            parameters['timeInForce'] = self.normalize_order_options(time_in_force)
        if stop_price:
            parameters['stopPrice'] = str(stop_price)
        if order_type == STOP_MARKET:
            parameters['closePosition'] = closePosition

        data = await self._request(POST, 'test' if test else 'order', auth=True, payload=parameters)
        return data
