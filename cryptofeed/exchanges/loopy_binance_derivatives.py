'''
Copyright (C) 2017-2021  Logan Hong - loopyluffy@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging
# import time
from typing import List, Tuple, Callable, Dict
from yapic import json

from cryptofeed.connection import AsyncConnection #, HTTPPoll
from cryptofeed.defines import (
    BALANCES, BINANCE_FUTURES, BINANCE_DELIVERY,
    PERPETUAL, FUTURES, SPOT, 
    TRADES, TICKER, FUNDING, OPEN_INTEREST, ORDER_INFO, POSITIONS, ACCOUNT_CONFIG, CANDLES,
    POST, DELETE, GET,
    ORDER_STATUS, CANCEL_ORDER, PLACE_ORDER, ORDERS,
    FILL_OR_KILL, GOOD_TIL_CANCELED, IMMEDIATE_OR_CANCEL,
    BUY, LIMIT, LIQUIDATIONS, MARKET, SELL, STOP_MARKET, STOP_LIMIT, TAKE_PROFIT_MARKET, TAKE_PROFIT_LIMIT, TRAILING_STOP_MARKET
)
from cryptofeed.types import OpenInterest, Ticker, Trade
from cryptofeed.loopy_types import LoopyOrderInfo, LoopyBalance, LoopyPosition
from cryptofeed.exchanges.binance import Binance
from cryptofeed.exchanges.mixins.binance_rest import BinanceRestMixin
from cryptofeed.symbols import Symbol, Symbols

LOG = logging.getLogger('feedhandler')


class LoopyBinanceDerivatives(Binance, BinanceRestMixin):
    listen_key_endpoint = 'listenKey'
    valid_depths = [5, 10, 20, 50, 100, 500, 1000]
    valid_depth_intervals = {'100ms', '250ms', '500ms'}
    order_options = {
        **BinanceRestMixin.order_options,
        STOP_LIMIT: 'STOP',
        STOP_MARKET: 'STOP_MARKET',
        TAKE_PROFIT_LIMIT: 'TAKE_PROFIT',
        TAKE_PROFIT_MARKET: 'TAKE_PROFIT_MARKET',
        TRAILING_STOP_MARKET: 'TRAILING_STOP_MARKET'
    }
    rest_channels = (
        TRADES, ORDER_STATUS, CANCEL_ORDER, PLACE_ORDER, BALANCES, ORDERS, POSITIONS
    )
    websocket_channels = {
        **Binance.websocket_channels,
        FUNDING: 'markPrice',
        OPEN_INTEREST: 'open_interest',
        LIQUIDATIONS: 'forceOrder',
        POSITIONS: POSITIONS
    }
    ticker_timestamp = 0
    trade_timestamp = 0

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        base, info = super()._parse_symbol_data(data)

        base_add = {}
        for symbol, orig in base.items():
            if "_" in orig:
                continue
            base_add[f"{symbol.replace('PERP', 'PINDEX')}"] = f"p{orig}"
        base.update(base_add)

        add = {}
        add['multiplier'] = {}
        add['price_precision'] = {}
        add['quantity_precision'] = {}
        
        for symbol in data['symbols']:
            if symbol.get('status', 'TRADING') != "TRADING":
                continue
            if symbol.get('contractStatus', 'TRADING') != "TRADING":
                continue

            expiration = None
            stype = SPOT
            if symbol.get('contractType') == 'PERPETUAL':
                stype = PERPETUAL
            elif symbol.get('contractType') in ('CURRENT_QUARTER', 'NEXT_QUARTER'):
                stype = FUTURES
                expiration = symbol['symbol'].split("_")[1]

            s = Symbol(symbol['baseAsset'], symbol['quoteAsset'], type=stype, expiry_date=expiration)
            if symbol.get('contractSize'):
                add['multiplier'][s.normalized] = symbol.get('contractSize') 
            if 'pricePrecision' in symbol:
                add['price_precision'][s.normalized] = symbol.get('pricePrecision') 
            if 'quantityPrecision' in symbol: 
                add['quantity_precision'][s.normalized] = symbol.get('quantityPrecision') 
                
        info.update(add)
        return base, info

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

    async def _trade(self, msg: dict, timestamp: float):
        # check ticker frequency
        if 'cache_write_wait' in self.config:
            interval = timestamp - self.trade_timestamp
            wait = self.config.cache_write_wait - interval
            # LOG.info(f'binance futures ticker wait: {wait}')
            if wait > 0:
                return
        
        # """
        # {
        #     "e": "aggTrade",  // Event type
        #     "E": 123456789,   // Event time
        #     "s": "BNBBTC",    // Symbol
        #     "a": 12345,       // Aggregate trade ID
        #     "p": "0.001",     // Price
        #     "q": "100",       // Quantity
        #     "f": 100,         // First trade ID
        #     "l": 105,         // Last trade ID
        #     "T": 123456785,   // Trade time
        #     "m": true,        // Is the buyer the market maker?
        #     "M": true         // Ignore
        # }
        # """
        # t = LoopyTrade(self.id,
        #                self.exchange_symbol_to_std_symbol(msg['s']),
        #                SELL if msg['m'] else BUY,
        #                Decimal(msg['q']),
        #                Decimal(msg['p']),
        #                self.timestamp_normalize(msg['E']),
        #                id=str(msg['a']),   # this string converting is very important to use avro format... @logan
        #                raw=msg)
        # await self.callback(TRADES, t, timestamp)
        await super()._trade(msg, timestamp)

    async def _ticker(self, msg: dict, timestamp: float):
        # check ticker frequency
        if 'cache_write_wait' in self.config:
            interval = timestamp - self.ticker_timestamp
            wait = self.config.cache_write_wait - interval
            # LOG.info(f'binance futures ticker wait: {wait}')
            if wait > 0:
                return
        
        await super()._ticker(msg, timestamp)

    async def _funding(self, msg: dict, timestamp: float):
        # check ticker frequency
        if 'cache_write_wait' in self.config:
            interval = timestamp - self.trade_timestamp
            wait = self.config.cache_write_wait - interval
            # LOG.info(f'binance futures ticker wait: {wait}')
            if wait > 0:
                return

        await super()._funding(msg, timestamp)

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
        # to sync callback parameters @logan
        for balance in msg['a']['B']:

            bal = LoopyBalance(
                exchange=self.id,
                currency=balance['a'],
                account=self.key_id,
                balance=Decimal(balance['wb']),
                cw_balance=Decimal(balance['cw']),
                # how can get reserved balance...? @logan
                reserved=Decimal(balance['wb']) - Decimal(0),
                changed=Decimal(balance['bc']),
                # timestamp=self.timestamp_normalize(msg['E'])
                timestamp=float(msg['E'])
                # raw=balance)
            )

            await self.callback(BALANCES, bal, timestamp)

        for position in msg['a']['P']:

            pos = LoopyPosition(
                exchange=self.id,
                symbol=self.exchange_symbol_to_std_symbol(position['s']),
                account=self.key_id,
                margin_type=position['mt'],
                side='long' if Decimal(position['pa']) > 0 else 'short' if Decimal(position['pa']) < 0 else 'none',
                # side=position['ps'],
                entry_price=Decimal(position['ep']),
                amount=Decimal(position['pa']),
                unrealised_pnl=Decimal(position['up']),
                cum_pnl=Decimal(position['cr']),
                # timestamp=self.timestamp_normalize(msg['E'])
                timestamp=float(msg['E'])
                # raw=balance)
            )

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
        oi = LoopyOrderInfo(
            exchange=self.id,
            symbol=self.exchange_symbol_to_std_symbol(msg['o']['s']),
            # in binance order id is number @logan
            id=str(msg['o']['i']),
            account=self.key_id,
            # id=msg['o']['i'],
            side=BUY if msg['o']['S'].lower() == 'buy' else SELL,
            status=msg['o']['X'],  # order status is not excution type @logan
            type=LIMIT if msg['o']['o'].lower() == 'limit' else MARKET if msg['o']['o'].lower() == 'market' else STOP_LIMIT if msg['o']['o'].lower() == 'stop' else STOP_MARKET if msg['o']['o'].lower() == 'stop_market' else TAKE_PROFIT_LIMIT if msg['o']['o'].lower() == 'take_profit' else TAKE_PROFIT_MARKET if msg['o']['o'].lower() == 'take_profit_market' else TRAILING_STOP_MARKET if msg['o']['o'].lower() == 'trailing_stop_market' else msg['o']['o'],
            # if never partially filled, price is original price... @logan
            price=Decimal(msg['o']['ap']) if not Decimal.is_zero(Decimal(msg['o']['ap'])) else Decimal(msg['o']['p']),
            # price=Decimal(msg['o']['ap']) if not Decimal.is_zero(Decimal(msg['o']['ap'])) else None,
            condition_price=Decimal(msg['o']['sp']) if msg['o']['o'].lower() == 'stop_market' or msg['o']['o'].lower() == 'take_profit_market' else Decimal(msg['o']['AP']) if msg['o']['o'].lower() == 'trailing_stop_market' else Decimal(0),
            amount=Decimal(msg['o']['q']),
            remaining=Decimal(msg['o']['q']) - Decimal(msg['o']['z']),
            # timestamp=self.timestamp_normalize(msg['E'])
            timestamp=float(msg['E'])
            # raw=msg
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

    # for various option orders... @logan
    async def place_order(self, symbol: str, side: str, order_type: str, amount: Decimal, price=None, reduce_only=None, stop_price=None, close_position=False,  time_in_force=None, callback_rate=None, test=False):
        if order_type == LIMIT:
            if not price:
                raise ValueError('Must specify price on a limit order')
            if not time_in_force:
                raise ValueError('Must specify time in force on a limit order') 
        if order_type == STOP_LIMIT or order_type == TAKE_PROFIT_LIMIT:
            if not price:
                raise ValueError('Must specify price on a stop order')
            if not stop_price:
                raise ValueError('Must specify stop_price on a stop order')
        if order_type == STOP_MARKET or order_type == TAKE_PROFIT_MARKET:
            if not stop_price:
                raise ValueError('Must specify stop_price on a stop market order')
        if (order_type == MARKET or order_type == STOP_MARKET or order_type == TAKE_PROFIT_MARKET) and price:
            raise ValueError('Cannot specify price on a market order')
        # if (order_type == TRAILING_STOP_MARKET) and (price or stop_price or time_in_force):
        # use stop_price for activation_price
        if (order_type == TRAILING_STOP_MARKET) and (price or time_in_force):
            raise ValueError('Cannot specify price on a trailing stop market order')
        if order_type == TRAILING_STOP_MARKET:
            if price or time_in_force:
                raise ValueError('Cannot specify price or time in force on a trailing stop market order')
            if not callback_rate:
                raise ValueError('Must specify callbackRate on a trailing stop market order')             
            
        ot = self.normalize_order_options(order_type)
        sym = self.std_symbol_to_exchange_symbol(symbol)
        parameters = {
            'symbol': sym,
            'side': 'BUY' if side is BUY else 'SELL',
            'type': ot,
            'quantity': str(amount),
        } 

        if price:
            if order_type == STOP_MARKET or order_type == TAKE_PROFIT_MARKET:
                parameters['stopPrice'] = str(price)
                # parameters['closePosition'] = close_position
            elif order_type != TRAILING_STOP_MARKET:
                parameters['price'] = str(price)
        if time_in_force:
            parameters['timeInForce'] = self.normalize_order_options(time_in_force)
        if stop_price:
            parameters['stopPrice'] = str(stop_price)
        if order_type == STOP_MARKET or order_type == STOP_LIMIT or order_type == TAKE_PROFIT_MARKET or order_type == TAKE_PROFIT_LIMIT:
            parameters['closePosition'] = close_position
        if reduce_only and close_position != True:
            parameters['reduceOnly'] = reduce_only
        if order_type == TRAILING_STOP_MARKET:
            parameters['callbackRate'] = callback_rate

        data = await self._request(POST, 'test' if test else 'order', auth=True, payload=parameters)
        return data

    # support contract precision
    def get_precision_order_price(self, symbol, price):
        info = self.info()
        ret = price
        LOG.info(f"{symbol}: tick size[{info['tick_size'][symbol]}], price precision[{info['price_precision'][symbol]}]")
        if 'tick_size' in info and symbol in info['tick_size']:
            tick_size = float(info['tick_size'][symbol])
            ret = price - price % tick_size + (0.0 if price % tick_size < tick_size / 2 else tick_size)
        if 'price_precision' in info and symbol in info['price_precision']:
            price_precision = info['price_precision'][symbol]
            ret = round(ret, price_precision)
            
        return ret

    def get_precision_order_quantity(self, symbol, quantity):
        info = self.info()
        if 'quantity_precision' in info and symbol in info['quantity_precision']:
            quantity_precision = self.info()['quantity_precision'][symbol]
            return round(quantity, quantity_precision)
        else:
            return quantity




