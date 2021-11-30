'''
Copyright (C) 2017-2021  Logan Hong - loopyluffy@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging

from cryptofeed.connection import AsyncConnection #, HTTPPoll
from cryptofeed.defines import (
    BALANCES, BINANCE_DELIVERY
    # PERPETUAL, FUTURES, SPOT, 
    # TRADES, TICKER, FUNDING, OPEN_INTEREST, ORDER_INFO, POSITIONS, ACCOUNT_CONFIG, 
    # POST,
    # BUY, LIMIT, LIQUIDATIONS, MARKET, SELL, STOP_MARKET, STOP_LIMIT, TAKE_PROFIT_MARKET, TAKE_PROFIT_LIMIT, TRAILING_STOP_MARKET
)
# from cryptofeed.exchanges.binance import Binance
# from cryptofeed.exchanges.mixins.binance_rest import BinanceFuturesRestMixin
# from cryptofeed.types import OpenInterest, Ticker #, Trade
# from cryptofeed.loopy_types import LoopyOrderInfo, LoopyBalance, LoopyPosition, LoopyTrade
# from cryptofeed.exchanges.mixins.binance_rest import BinanceDeliveryRestMixin
from cryptofeed.exchanges.loopy_binance_derivatives import LoopyBinanceDerivatives

LOG = logging.getLogger('feedhandler')


class LoopyBinanceDelivery(LoopyBinanceDerivatives):
    id = BINANCE_DELIVERY
    symbol_endpoint = 'https://dapi.binance.com/dapi/v1/exchangeInfo'
    websocket_endpoint = 'wss://dstream.binance.com'
    api = 'https://dapi.binance.com/dapi/v1/'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # overwrite values previously set by the super class Binance
        self.rest_endpoint = 'https://dapi.binance.com/dapi/v1'
        self.address = self._address()
        self.ws_defaults['compression'] = None