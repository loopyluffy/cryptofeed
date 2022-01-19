'''
Copyright (C) 2018-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from datetime import datetime
from decimal import Decimal





# import cryptofeed directly from local directory @logan
import os
import sys
cur_dir_name = os.path.dirname(__file__)
parent_dir_path = os.path.dirname(cur_dir_name)
sys.path.append(parent_dir_path)
# repo_dir_path = os.path.dirname(parent_dir_path)
# cryptofeed_dir_path = repo_dir_path + '/cryptofeed@loopyluffy'
# sys.path.append(cryptofeed_dir_path)

# to import pyx file @logan
import pyximport
pyximport.install()

import asyncio
import aiohttp

from cryptofeed import FeedHandler
from cryptofeed.defines import (BALANCES, BINANCE_FUTURES, BINANCE_DELIVERY,
                                FUNDING, LIQUIDATIONS, OPEN_INTEREST, ORDER_INFO, POSITIONS, ACCOUNT_CONFIG,
                                BUY, LIMIT, MARKET, SELL, STOP_MARKET, STOP_LIMIT, TRAILING_STOP_MARKET, GOOD_TIL_CANCELED)
from cryptofeed.exchanges import Binance, LoopyBinanceDelivery, LoopyBinanceFutures

from cryptofeed.symbols import Symbol, Symbols


delivery_info = LoopyBinanceDelivery.info()
futures_info = LoopyBinanceFutures.info()


async def abook(book, receipt_timestamp):
    print(f'BOOK lag: {receipt_timestamp - book.timestamp} Timestamp: {datetime.fromtimestamp(book.timestamp)} Receipt Timestamp: {datetime.fromtimestamp(receipt_timestamp)}')


async def ticker(t, receipt_timestamp):
    if t.timestamp is not None:
        assert isinstance(t.timestamp, float)
    assert isinstance(t.exchange, str)
    assert isinstance(t.bid, Decimal)
    assert isinstance(t.ask, Decimal)
    print(f'Ticker received at {receipt_timestamp}: {t}')


async def trades(t, receipt_timestamp):
    assert isinstance(t.timestamp, float)
    assert isinstance(t.side, str)
    assert isinstance(t.amount, Decimal)
    assert isinstance(t.price, Decimal)
    assert isinstance(t.exchange, str)
    print(f"Trade received at {receipt_timestamp}: {t}")

async def account(t, receipt_timestamp):
    # assert isinstance(t.timestamp, float)
    # assert isinstance(t.side, str)
    # assert isinstance(t.amount, Decimal)
    # assert isinstance(t.price, Decimal)
    # assert isinstance(t.exchange, str)
    print(f"Trade received at {receipt_timestamp}: {t}")


# def main():
async def main():
    # path_to_config = 'config.yaml'
    path_to_config = 'sandbox/rest_config.yaml'
    # binance = Binance(config=path_to_config)
    # print(binance.balances_sync())
    # print(binance.orders_sync())
    # order = binance.place_order_sync('BTC-USDT', SELL, LIMIT, 0.002, 80000, time_in_force=GOOD_TIL_CANCELED, test=False)
    # print(binance.orders_sync(symbol='BTC-USDT'))
    # print(order)
    # print(binance.cancel_order_sync(order['orderId'], symbol='BTC-USDT'))
    # print(binance.orders_sync(symbol='BTC-USDT'))

    # user data stream channel @logan
    # USER = "jzhJ53lZGnfvwZ0fnSARhZBnFkG12ScT7rPdRFYXGtIEzlgwvzbKsibMpk5njdBN"
    # USER_DATA = 'userData'

    binance_futures = LoopyBinanceFutures(config=path_to_config)
    # binance_delivery = LoopyBinanceDelivery(config=path_to_config)
    # print(BINANCE_FUTURES)
    # print(futures_info)
    # print(binance_futures.balances_sync())
    # print(binance_futures.orders_sync())
    # print(binance_futures.positions_sync())
    try:
        quantity = binance_futures.get_precision_order_quantity('ADA-USDT-PERP', 7)
        orig_price = 3.34019002
        price = binance_futures.get_precision_order_price('ADA-USDT-PERP', orig_price)
        print(f'ADA-USDT-PERP [orginal price: {orig_price} refined price: {price}')
        # order = binance_futures.place_order_sync('ADA-USDT-PERP', SELL, LIMIT, price, quantity, time_in_force=GOOD_TIL_CANCELED)
        # order = await asyncio.create_task(binance_futures.place_order(symbol='ADA-USDT-PERP', side=SELL, order_type=LIMIT, price=price, amount=quantity, time_in_force=GOOD_TIL_CANCELED))
        # order = await asyncio.create_task(binance_futures.place_order(symbol='BTC-USDT-PERP', side=BUY, order_type=STOP_MARKET, amount=0.001, stop_price=50000))
        order = await asyncio.create_task(binance_futures.place_order(symbol='BTC-USDT-PERP', side=BUY, order_type=STOP_LIMIT, amount=0.001, price=50000, stop_price=50000))
        # order = await asyncio.create_task(binance_delivery.place_order(symbol='ETH-USD-PERP', side=SELL, order_type=TRAILING_STOP_MARKET, amount=1, reduce_only=True, callback_rate=0.8))
        # print(binance_futures.orders_sync(symbol='BTC-USDT-PERP'))
        # print(binance_futures.orders_sync(symbol='ETH-USDT-PERP'))
        print(order)
        # print(binance_futures.cancel_order_sync(order['orderId'], symbol='ETH-USDT-PERP'))
        # print(binance_futures.orders_sync(symbol='ETH-USDT-PERP'))
    except Exception as e:
    # except aiohttp.client_exceptions.ClientResponseError as e:
        # if e.status == 400:
        print(e)
            # order = await asyncio.create_task(binance_futures.place_order(symbol='BTC-USDT-PERP', side=BUY, order_type=STOP_MARKET, amount=0.001, stop_price=70000))
        # else:
            # print('exception not 400..')
    # except aiohttp.client_exceptions.ClientResponseError as e:
    #     print(e)

    # print('reach here??')
    # except aiohttp.client_exceptions.ClientResponseError as e:
    #     if e.status == 409:  # Subscription exists
    #         pass
    #     else:
    #         raise TypeError("Please set the GCP_PROJECT environment variable") from e

    # binance_delivery = LoopyBinanceDelivery(config=path_to_config)
    # print(BINANCE_DELIVERY)
    # print(delivery_info)
    # print(Symbols.get(BINANCE_DELIVERY))

    # print(binance_delivery.balances_sync())
    # print(binance_delivery.orders_sync())
    # print(binance_delivery.positions_sync())
    # order = binance_delivery.place_order_sync('ETH-USD-PERP', SELL, LIMIT, 0.05, 5000, time_in_force=GOOD_TIL_CANCELED, test=False)
    # print(binance_delivery.orders_sync(symbol='BTC-USDT-PERP'))
    # print(binance_delivery.orders_sync(symbol='ETH-USDT-PERP'))
    # print(order)
    # print(binance_delivery.cancel_order_sync(order['orderId'], symbol='ETH-USDT-PERP'))
    # print(binance_delivery.orders_sync(symbol='ETH-USDT-PERP'))

    '''
    f = FeedHandler()
    # f.add_feed(BinanceDelivery(max_depth=3, symbols=[info['symbols'][-1]],
    #                            channels=[L2_BOOK, TRADES, TICKER],
    #                            callbacks={L2_BOOK: abook, TRADES: trades, TICKER: ticker}))
    f.add_feed(BinanceFutures(config=path_to_config,
                              max_depth=3, symbols=['BTC-USDT-PERP'],
                            #   channels=[USER_DATA],
                            #   callbacks={USER_DATA: account}))
                            #   channels=[TICKER, USER_DATA],
                            #   callbacks={TICKER: ticker, USER_DATA: account}))
                              channels=[TICKER],
                              callbacks={TICKER: ticker}))
                            # channels=[L2_BOOK, TRADES, TICKER],
                            # callbacks={L2_BOOK: abook, TRADES: trades, TICKER: ticker}))
    f.run()
    '''

if __name__ == '__main__':
    # main()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
