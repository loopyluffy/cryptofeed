'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
# from decimal import Decimal
# from functools import wraps
# from time import sleep

# import pandas as pd
# import requests

# from cryptofeed.standards import load_exchange_symbol_mapping

from cryptofeed.rest.api import API
from cryptofeed.rest.luffy_api_callback import APICallback


LOG = logging.getLogger('rest')


class APIFeed(API):

    def __init__(self, **kwargs):
        super().__init__(self, callbacks=None, **kwargs)

        self.callbacks = {
                            ORDERS: APICallback(None)
                         }

    if callbacks:
        for cb_type, cb_func in callbacks.items():
            self.callbacks[cb_type] = cb_func

    # for key, callback in self.callbacks.items():
    #     if not isinstance(callback, list):
    #         self.callbacks[key] = [callback]

    # public / non account specific
    # def ticker(self, symbol: str, retry=None, retry_wait=10):
    #     raise NotImplementedError

    # def trades(self, symbol: str, start=None, end=None, retry=None, retry_wait=0):
    #     raise NotImplementedError

    # def funding(self, symbol: str, retry=None, retry_wait=0):
    #     raise NotImplementedError

    # def l2_book(self, symbol: str, retry=None, retry_wait=0):
    #     raise NotImplementedError

    # def l3_book(self, symbol: str, retry=None, retry_wait=0):
    #     raise NotImplementedError

    # message handler
    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        raise NotImplementedError

    async def callback(self, channel, **kwargs):
        for cb in self.callbacks[channel]:
            await cb(**kwargs)

    # account specific
    # def place_order(self, symbol: str, side: str, order_type: str, amount: Decimal, price=None, **kwargs):
    #     raise NotImplementedError

    # def cancel_order(self, order_id: str):
    #     raise NotImplementedError

    # def orders(self):
    #     """
    #     Return outstanding orders
    #     """
    #     raise NotImplementedError

    # def order_status(self, order_id: str):
    #     """
    #     Look up status of an order by id
    #     """
    #     raise NotImplementedError

    # def trade_history(self, symbol: str, start=None, end=None):
    #     """
    #     Executed trade history
    #     """
    #     raise NotImplementedError

    # def balances(self):
    #     raise NotImplementedError

    # def __getitem__(self, key):
    #     if not self.mapped:
    #         try:
    #             load_exchange_symbol_mapping(self.ID + 'REST')
    #         except KeyError:
    #             load_exchange_symbol_mapping(self.ID)
    #         self.mapped = True
    #     if key == 'trades':
    #         return self.trades
    #     elif key == 'funding':
    #         return self.funding
    #     elif key == 'l2_book':
    #         return self.l2_book
    #     elif key == 'l3_book':
    #         return self.l3_book
    #     elif key == 'ticker':
    #         return self.ticker
