'''
Copyright (C) 2017-2021  Logan Hong - loopyluffy@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging

from cryptofeed.connection import RestEndpoint, Routes, WebsocketEndpoint
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

    api = 'https://dapi.binance.com/dapi/v1/'
    websocket_endpoints = [WebsocketEndpoint('wss://dstream.binance.com', options={'compression': None})]
    rest_endpoints = [RestEndpoint('https://dapi.binance.com', routes=Routes('/dapi/v1/exchangeInfo', l2book='/dapi/v1/depth?symbol={}&limit={}', authentication='/dapi/v1/listenKey'))]


   