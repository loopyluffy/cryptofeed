'''
Copyright (C) 2017-2021  loopyluffy@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
# from collections import defaultdict
# import asyncio

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# from aiokafka import AIOKafkaProducer
# from yapic import json
from uuid import uuid4

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback
from cryptofeed.backends.kafka import KafkaCallback

import logging
LOG = logging.getLogger('feedhandler')


class LoopyAvroKafkaCallback(KafkaCallback):
    def __init__(self, bootstrap='127.0.0.1', port=9092, schema_registry_ip=None, schema_registry_port=None, key=None, numeric_type=float, none_to=None, **kwargs):
        assert isinstance(schema_registry_ip, str)
        assert isinstance(schema_registry_port, int)

        super().__init__(bootstrap=bootstrap, port=port, key=key, numeric_type=numeric_type, none_to=none_to, kwargs=kwargs)

        self.schema_registry_conf = {'url': f'http://{schema_registry_ip}:{schema_registry_port}'}

    def __connect(self): 
        if not self.producer:
            # schema_registry_conf = {'url': f'{self.schema_registry_ip}:{self.schema_registry_port}'}
            schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

            avro_serializer = AvroSerializer(schema_str=self.schema_str,
                                             schema_registry_client=schema_registry_client)

            producer_conf = {'bootstrap.servers': f'{self.bootstrap}:{self.port}',
                            'key.serializer': StringSerializer('utf_8'),
                            'value.serializer': avro_serializer}

            self.producer = SerializingProducer(producer_conf)

    async def write(self, data: dict):
        self.__connect()

        # exclude exchange in topic name
        # topic = f"{self.key}".lower()
        topic = f"{self.key}-{data['exchange']}".lower()

        # LOG.info(f'{topic}: {data}')
        
        # Serve on_delivery callbacks from previous calls to produce()
        # self.producer.poll(0.0)
        self.producer.produce(topic=topic, key=str(uuid4()), value=data)
        # self.producer.produce(topic=topic, key=str(uuid4()), value=json.dumps(data).encode('utf-8'))
                            #   on_delivery=delivery_report)
        self.producer.flush()


# user data channel @logan
class LoopyBalancesKafka(LoopyAvroKafkaCallback, BackendCallback):
    default_key = 'balances'
    default_channel = 'balances'
    schema_str = """
    {
        "namespace": "loopyluffy.serialization.avro",
        "name": "Balance",
        "type": "record",
        "fields": [
            {"name": "exchange", "type": "string"},
            {"name": "currency", "type": "string"},
            {"name": "account", "type": "string"},
            {"name": "balance", "type": "float"},
            {"name": "cw_balance", "type": "float"},
            {"name": "changed", "type": "float"},
            {"name": "timestamp", "type": "float"},
            {"name": "receipt_timestamp", "type": "float"}
        ]
    }
    """


class LoopyPositionsKafka(LoopyAvroKafkaCallback, BackendCallback):
    default_key = 'positions'
    default_channel = 'positions'
    schema_str = """
    {
        "namespace": "loopyluffy.serialization.avro",
        "name": "Position",
        "type": "record",
        "fields": [
            {"name": "exchange", "type": "string"},
            {"name": "symbol", "type": "string"},
            {"name": "account", "type": "string"},
            {"name": "id", "type": "string"},
            {"name": "margin_type", "type": "string", "default": ""},
            {"name": "side", "type": "string", "default": ""},
            {"name": "entry_price", "type": "float", "default": 0},
            {"name": "amount", "type": "float", "default": 0},
            {"name": "unrealised_pnl", "type": "float", "default": 0},
            {"name": "cum_pnl", "type": "float", "default": 0},
            {"name": "timestamp", "type": "float"},
            {"name": "receipt_timestamp", "type": "float"}
        ]
    }
    """


# class LoopyAccountConfigKafka(KafkaCallback, BackendCallback):
#     default_key = 'account_config'
#     default_channel = 'account_config'


class LoopyOrderInfoKafka(LoopyAvroKafkaCallback, BackendCallback):
    default_key = 'order_info'
    default_channel = 'order_info'
    schema_str = """
    {
        "namespace": "loopyluffy.serialization.avro",
        "name": "OrderInfo",
        "type": "record",
        "fields": [
            {"name": "exchange", "type": "string"},
            {"name": "symbol", "type": "string"},
            {"name": "id", "type": "string"},
            {"name": "account", "type": "string"},
            {"name": "position", "type": "string"},
            {"name": "side", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "type", "type": "string"},
            {"name": "price", "type": "float"},
            {"name": "condition_price", "type": "float"},
            {"name": "amount", "type": "float"},
            {"name": "remaining", "type": "float"},
            {"name": "timestamp", "type": "float"},
            {"name": "receipt_timestamp", "type": "float"}
        ]
    }
    """


class LoopyFundingKafka(LoopyAvroKafkaCallback, BackendCallback):
    default_key = 'funding'
    default_channel = 'funding'
    schema_str = """
    {
        "namespace": "loopyluffy.serialization.avro",
        "name": "Funding",
        "type": "record",
        "fields": [
            {"name": "exchange", "type": "string"},
            {"name": "symbol", "type": "string"},
            {"name": "mark_price", "type": "float"},
            {"name": "rate", "type": "float"},
            {"name": "next_funding_time", "type": "float", "default": 0},
            {"name": "predicted_rate", "type": "float"},
            {"name": "timestamp", "type": "float"},
            {"name": "receipt_timestamp", "type": "float"}
        ]
    }
    """


class LoopyTickerKafka(LoopyAvroKafkaCallback, BackendCallback):
    default_key = 'ticker'
    default_channel = 'ticker'
    schema_str = """
    {
        "namespace": "loopyluffy.serialization.avro",
        "name": "Ticker",
        "type": "record",
        "fields": [
            {"name": "exchange", "type": "string"},
            {"name": "symbol", "type": "string"},
            {"name": "bid", "type": "float"},
            {"name": "ask", "type": "float"},
            {"name": "timestamp", "type": "float"},
            {"name": "receipt_timestamp", "type": "float"}
        ]
    }
    """


class LoopyTradeKafka(LoopyAvroKafkaCallback, BackendCallback):
    default_key = 'trades'
    default_channel = 'trades'
    schema_str = """
    {
        "namespace": "loopyluffy.serialization.avro",
        "name": "Trade",
        "type": "record",
        "fields": [
            {"name": "exchange", "type": "string"},
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "float"},
            {"name": "amount", "type": "float"},
            {"name": "side", "type": "string"},
            {"name": "id", "type": "string", "default": ""},
            {"name": "type", "type": "string", "default": ""},
            {"name": "timestamp", "type": "float"},
            {"name": "receipt_timestamp", "type": "float"}
        ]
    }
    """



   
