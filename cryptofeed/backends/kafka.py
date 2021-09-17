'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio

from aiokafka import AIOKafkaProducer
from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback


class KafkaCallback:
    def __init__(self, bootstrap='127.0.0.1', port=9092, key=None, numeric_type=float, **kwargs):
        self.bootstrap = bootstrap
        self.port = port
        self.producer = None
        self.key = key if key else self.default_key
        self.numeric_type = numeric_type

    async def __connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            self.producer = AIOKafkaProducer(acks=0,
                                             loop=loop,
                                             bootstrap_servers=f'{self.bootstrap}:{self.port}',
                                             client_id='cryptofeed')
            await self.producer.start()

    async def write(self, data: dict):
        await self.__connect()
        # topic = f"{self.key}-{feed}-{symbol}"
        # define new topic convention @logan
        # topic keywords lowercase only @weaver
        # topic =  f"{self.key}norm-live-{feed}-{self.default_channel}-{symbol}".lower()
        # topic = f"{self.key}-{data['exchange']}-{data['symbol']}"
        topic = f"{self.key}-{data['exchange']}-{data['symbol']}".lower()
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

    def __init__(self, *args, snapshots_only=False, **kwargs):
        self.snapshots_only = snapshots_only
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
