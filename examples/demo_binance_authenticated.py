
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
from cryptofeed.defines import BALANCES, POSITIONS
from cryptofeed.exchanges import Binance, BinanceDelivery, BinanceFutures


async def balance(**kwargs):
    print(f"Balance: {kwargs}")


async def position(**kwargs):
    print(f"Position: {kwargs}")


def main():
    # path_to_config = 'config.yaml'
    path_to_config = 'sandbox/rest_config.yaml'

    # binance = Binance(config=path_to_config, subscription={BALANCES: []}, timeout=-1, callbacks={BALANCES: balance})
    # binance_delivery = BinanceDelivery(config=path_to_config, subscription={BALANCES: [], POSITIONS: []}, timeout=-1, callbacks={BALANCES: balance, POSITIONS: position})
    binance_futures = BinanceFutures(config=path_to_config, subscription={BALANCES: [], POSITIONS: []}, timeout=-1, callbacks={BALANCES: balance, POSITIONS: position})

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
