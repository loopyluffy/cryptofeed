'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

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



from cryptofeed import FeedHandler
from cryptofeed.defines import GOOD_TIL_CANCELED, L2_BOOK, LIMIT, SELL, TICKER, TRADES, BALANCES
from cryptofeed.exchanges import Binance, BinanceDelivery, BinanceFutures


# info = BinanceDelivery.info()
info = BinanceFutures.info()


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


def main():
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

    binance_futures = BinanceFutures(config=path_to_config)
    print(binance_futures.balances_sync())
    print(binance_futures.orders_sync())
    print(binance_futures.positions_sync())
    order = binance_futures.place_order_sync('ETH-USDT-PERP', SELL, LIMIT, 0.004, 5000, time_in_force=GOOD_TIL_CANCELED)
    print(binance_futures.orders_sync(symbol='BTC-USDT-PERP'))
    print(binance_futures.orders_sync(symbol='ETH-USDT-PERP'))
    print(order)
    print(binance_futures.cancel_order_sync(order['orderId'], symbol='ETH-USDT-PERP'))
    print(binance_futures.orders_sync(symbol='ETH-USDT-PERP'))

    # binance_delivery = BinanceDelivery(config=path_to_config)
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
    main()
