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

from cryptofeed.connection import AsyncConnection, HTTPPoll, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import (
    BALANCES, BINANCE_FUTURES, 
    # PERPETUAL, FUTURES, SPOT, 
    TRADES, TICKER, FUNDING, OPEN_INTEREST, ORDER_INFO, POSITIONS, ACCOUNT_CONFIG
    # POST,
    # BUY, LIMIT, LIQUIDATIONS, MARKET, SELL, STOP_MARKET, STOP_LIMIT, TAKE_PROFIT_MARKET, TAKE_PROFIT_LIMIT, TRAILING_STOP_MARKET
)
# from cryptofeed.types import OpenInterest, Ticker, Trade
# from cryptofeed.loopy_types import LoopyOrderInfo, LoopyBalance, LoopyPosition
from cryptofeed.exchanges.loopy_binance_derivatives import LoopyBinanceDerivatives
# from cryptofeed.exchanges.mixins.binance_rest import BinanceFuturesRestMixin
# from cryptofeed.symbols import Symbol, Symbols

LOG = logging.getLogger('feedhandler')


class LoopyBinanceFutures(LoopyBinanceDerivatives):
    id = BINANCE_FUTURES

    api = 'https://fapi.binance.com/fapi/v1/'
    websocket_endpoints = [WebsocketEndpoint('wss://fstream.binance.com', sandbox='wss://stream.binancefuture.com', options={'compression': None})]
    rest_endpoints = [RestEndpoint('https://fapi.binance.com', sandbox='https://testnet.binancefuture.com', routes=Routes('/fapi/v1/exchangeInfo', l2book='/fapi/v1/depth?symbol={}&limit={}', authentication='/fapi/v1/listenKey', open_interest='/fapi/v1//openInterest?symbol={}'))]

    def __init__(self, open_interest_interval=1.0, **kwargs):
        """
        open_interest_interval: flaot
            time in seconds between open_interest polls
        """
        super().__init__(**kwargs)

        self.open_interest_interval = open_interest_interval

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

    def _connect_rest(self):
        ret = []
        for chan in set(self.subscription):
            if chan == 'open_interest':
                addrs = [self.rest_endpoints[0].route('open_interest', sandbox=self.sandbox).format(pair) for pair in self.subscription[chan]]
                ret.append((HTTPPoll(addrs, self.id, delay=60.0, sleep=self.open_interest_interval, proxy=self.http_proxy), self.subscribe, self.message_handler, self.authenticate))
        return ret