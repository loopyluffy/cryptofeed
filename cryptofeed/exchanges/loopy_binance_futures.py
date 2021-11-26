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

from cryptofeed.connection import AsyncConnection, HTTPPoll
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
    symbol_endpoint = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    api = 'https://fapi.binance.com/fapi/v1/'

    def __init__(self, open_interest_interval=1.0, **kwargs):
        """
        open_interest_interval: flaot
            time in seconds between open_interest polls
        """
        super().__init__(**kwargs)
        # overwrite values previously set by the super class Binance
        self.ws_endpoint = 'wss://fstream.binance.com' if not self.sandbox else "wss://stream.binancefuture.com"
        self.rest_endpoint = 'https://fapi.binance.com/fapi/v1' if not self.sandbox else "https://testnet.binancefuture.com/fapi/v1"
        self.address = self._address()
        self.ws_defaults['compression'] = None

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

    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        ret = []
        if self.address:
            ret = super().connect()
        PollCls = HTTPPoll
        for chan in set(self.subscription):
            if chan == 'open_interest':
                addrs = [f"{self.rest_endpoint}/openInterest?symbol={pair}" for pair in self.subscription[chan]]
                ret.append((PollCls(addrs, self.id, delay=60.0, sleep=self.open_interest_interval, proxy=self.http_proxy), self.subscribe, self.message_handler, self.authenticate))
        return ret
