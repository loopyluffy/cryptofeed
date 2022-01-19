'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
import asyncio

from aiokafka import AIOKafkaProducer
from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback


class KafkaCallback:
    def __init__(self, bootstrap='127.0.0.1', port=9092, key=None, numeric_type=float, none_to=None, **kwargs):
        """
        bootstrap: str, list
            if a list, should be a list of strings in the format: ip/host:port, i.e.
                192.1.1.1:9092
                192.1.1.2:9092
                etc
            if a string, should be ip/port only
        """
        self.bootstrap = bootstrap
        self.port = port
        self.producer = None
        self.key = key if key else self.default_key
        self.numeric_type = numeric_type
        self.none_to = none_to

    async def __connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            self.producer = AIOKafkaProducer(acks=0,
                                             loop=loop,
                                             bootstrap_servers=f'{self.bootstrap}:{self.port}' if isinstance(self.bootstrap, str) else self.bootstrap,
                                             client_id='cryptofeed')
            await self.producer.start()

    async def write(self, data: dict):
        await self.__connect()
        # topic = f"{self.key}-{feed}-{symbol}"
        # define new topic convention @logan
        # topic keywords lowercase only @weaver
        # topic =  f"{self.key}norm-live-{feed}-{self.default_channel}-{symbol}".lower()
        # topic = f"{self.key}-{data['exchange']}-{data['symbol']}"
        # topic = f"{self.key}-{data['exchange']}-{data['symbol']}".lower()
        topic = f"{self.key}-{data['exchange']}".lower()
        await self.producer.send_and_wait(topic, json.dumps(data).encode('utf-8'))


class TradeKafka(KafkaCallback, BackendCallback):
    default_key = 'trades'
    # for key to use name convention, data_type needed... @logan
    default_channel = 'trades'


class FundingKafka(KafkaCallback, BackendCallback):
    default_key = 'funding'
    default_channel = 'funding'


class BookKafka(KafkaCallback, BackendBookCallback):
    default_key = 'book'
    default_channel = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)


class TickerKafka(KafkaCallback, BackendCallback):
    default_key = 'ticker'
    default_channel = 'ticker'


class OpenInterestKafka(KafkaCallback, BackendCallback):
    default_key = 'open_interest'
    default_channel = 'open_interest'


class LiquidationsKafka(KafkaCallback, BackendCallback):
    default_key = 'liquidations'
    default_channel = 'liquidations'


class CandlesKafka(KafkaCallback, BackendCallback):
    default_key = 'candles'
    default_channel = 'candles'


# user data channel @logan
class BalancesKafka(KafkaCallback, BackendCallback):
    default_key = 'balances'
    default_channel = 'balances'


class PositionsKafka(KafkaCallback, BackendCallback):
    default_key = 'positions'
    default_channel = 'positions'


class AccountConfigKafka(KafkaCallback, BackendCallback):
    default_key = 'account_config'
    default_channel = 'account_config'


class OrderInfoKafka(KafkaCallback, BackendCallback):
    default_key = 'order_info'
    default_channel = 'order_info'
