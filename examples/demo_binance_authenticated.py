
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
from cryptofeed.defines import BALANCES, ORDER_INFO, POSITIONS
from cryptofeed.exchanges import Binance, BinanceDelivery, BinanceFutures


async def balance(balance, receipt):
    print(f"Balance: {balance}")


async def position(position, receipt):
    print(f"Position: {position}")


async def order_info(order, receipt):
    print(f"Order info: {order}")


def main():
    # path_to_config = 'config.yaml'
    path_to_config = 'sandbox/rest_config.yaml'

    # binance = Binance(config=path_to_config, subscription={BALANCES: []}, timeout=-1, callbacks={BALANCES: balance, ORDER_INFO: order_info})
    # binance_delivery = BinanceDelivery(config=path_to_config, subscription={BALANCES: [], POSITIONS: []}, timeout=-1, callbacks={BALANCES: balance, POSITIONS: position, ORDER_INFO: order_info})
    binance_futures = BinanceFutures(config=path_to_config, subscription={BALANCES: [], POSITIONS: []}, timeout=-1, callbacks={BALANCES: balance, POSITIONS: position, ORDER_INFO: order_info})

    # print(binance._generate_token())
    # print(binance_delivery._generate_token())
    print(binance_futures._generate_token())

    f = FeedHandler()
    # f.add_feed(binance)
    # f.add_feed(binance_delivery)
    f.add_feed(binance_futures)
    f.run()


if __name__ == '__main__':
    main()
