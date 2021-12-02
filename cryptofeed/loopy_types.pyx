'''
Copyright (C) 2017-2021  loopyluffy@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import hashlib

# from cryptofeed.types import OrderInfo, Balance, Position


cdef extern from *:
    """
    #ifdef CYTHON_WITHOUT_ASSERTIONS
    #define _COMPILED_WITH_ASSERTIONS 0
    #else
    #define _COMPILED_WITH_ASSERTIONS 1
    #endif
    """
    cdef bint _COMPILED_WITH_ASSERTIONS
COMPILED_WITH_ASSERTIONS = _COMPILED_WITH_ASSERTIONS


cdef dict convert_none_values(d: dict, s: str):
    for key, value in d.items():
        if value is None:
            d[key] = s
    return d

cdef str convert_hash(s: str):
    return hashlib.sha256(s.encode()).hexdigest()


cdef class OrderInfo:
    cdef readonly str exchange
    cdef readonly str symbol
    cdef readonly str id
    cdef readonly str side
    cdef readonly str status
    cdef readonly str type
    cdef readonly object price
    cdef readonly object amount
    cdef readonly object remaining
    cdef readonly str account
    cdef readonly object timestamp
    cdef readonly object raw  # Can be dict or list

    def __init__(self, exchange, symbol, id, side, status, type, price, amount, remaining, timestamp, account=None, raw=None):
        assert isinstance(price, Decimal)
        assert isinstance(amount, Decimal)
        assert remaining is None or isinstance(remaining, Decimal)
        assert timestamp is None or isinstance(timestamp, float)

        self.exchange = exchange
        self.symbol = symbol
        self.id = id
        self.side = side
        self.status = status
        self.type = type
        self.price = price
        self.amount = amount
        self.remaining = remaining
        self.account = account
        self.timestamp = timestamp
        self.raw = raw

    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'id': self.id, 'side': self.side, 'status': self.status, 'type': self.type, 'price': self.price, 'amount': self.amount, 'remaining': self.remaining, 'account': self.account, 'timestamp': self.timestamp}
        else:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'id': self.id, 'side': self.side, 'status': self.status, 'type': self.type, 'price': numeric_type(self.price), 'amount': numeric_type(self.amount), 'remaining': numeric_type(self.remaining), 'account': self.account, 'timestamp': self.timestamp}
        return data if not none_to else convert_none_values(data, none_to)

    def __repr__(self):
        return f'exchange: {self.exchange} symbol: {self.symbol} id: {self.id} side: {self.side} status: {self.status} type: {self.type} price: {self.price} amount: {self.amount} remaining: {self.remaining} account: {self.account} timestamp: {self.timestamp}'

    def __eq__(self, cmp):
        return self.exchange == cmp.exchange and self.symbol == cmp.symbol and self.id == cmp.id and self.status == cmp.status and self.type == cmp.type and self.price == cmp.price and self.amount == cmp.amount and self.remaining == cmp.remaining and self.timestamp == cmp.timestamp and self.account == cmp.account

    def __hash__(self):
        return hash(self.__repr__())


cdef class LoopyOrderInfo(OrderInfo):
    # cdef readonly str account
    cdef readonly str position
    cdef readonly object condition_price

    def __init__(self, exchange, symbol, id, account, side, status, type, price, amount, remaining, timestamp, condition_price=None, position=None, raw=None):
        assert condition_price is None or isinstance(condition_price, Decimal)

        super().__init__(exchange, symbol, id, side, status, type, price, amount, remaining, timestamp, convert_hash(account), raw)

        # self.account = convert_hash(account)
        self.condition_price = condition_price if condition_price is not None else 0
        self.position = position if position is not None else self.account 

    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'id': self.id, 'account': self.account, 'position': self.position, 'side': self.side, 'status': self.status, 'type': self.type, 'price': self.price, 'amount': self.amount, 'remaining': self.remaining, 'condition_price': self.condition_price, 'timestamp': self.timestamp}
        else:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'id': self.id, 'account': self.account, 'position': self.position, 'side': self.side, 'status': self.status, 'type': self.type, 'price': numeric_type(self.price), 'amount': numeric_type(self.amount), 'remaining': numeric_type(self.remaining), 'condition_price': numeric_type(self.condition_price), 'timestamp': self.timestamp}
        return data if not none_to else convert_none_values(data, none_to)

    def __repr__(self):
        return f'exchange: {self.exchange} symbol: {self.symbol} id: {self.id} account: {self.account} position: {self.position} side: {self.side} status: {self.status} type: {self.type} price: {self.price} amount: {self.amount} remaining: {self.remaining} condition_price: {self.condition_price} timestamp: {self.timestamp}'


cdef class Balance:
    cdef readonly str exchange
    cdef readonly str currency
    cdef readonly object balance
    cdef readonly object reserved
    cdef readonly dict raw

    def __init__(self, exchange, currency, balance, reserved, raw=None):
        assert isinstance(balance, Decimal)
        assert reserved is None or isinstance(reserved, Decimal)

        self.exchange = exchange
        self.currency = currency
        self.balance = balance
        self.reserved = reserved
        self.raw = raw

    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data = {'exchange': self.exchange, 'currency': self.currency, 'balance': self.balance, 'reserved': self.reserved}
        else:
            data = {'exchange': self.exchange, 'currency': self.currency, 'balance': numeric_type(self.balance), 'reserved': numeric_type(self.reserved)}
        return data if not none_to else convert_none_values(data, none_to)

    def __repr__(self):
        return f'exchange: {self.exchange} currency: {self.currency} balance: {self.balance} reserved: {self.reserved}'

    def __eq__(self, cmp):
        return self.exchange == cmp.exchange and self.currency == cmp.currency and self.balance == cmp.balance and self.reserved == cmp.reserved

    def __hash__(self):
        return hash(self.__repr__())


cdef class LoopyBalance(Balance):
    cdef readonly str account
    cdef readonly object cw_balance  # cross wallet balance
    cdef readonly object changed    # balance change except pnl and commission
    cdef readonly object timestamp

    def __init__(self, exchange, currency, account, balance, cw_balance, reserved, changed, timestamp, raw=None):
        assert isinstance(cw_balance, Decimal)
        assert changed is None or isinstance(changed, Decimal)
        assert timestamp is None or isinstance(timestamp, float)

        super().__init__(exchange, currency, balance, reserved, raw)

        self.account = convert_hash(account)
        self.cw_balance = cw_balance
        self.changed = changed 
        self.timestamp = timestamp

    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data =  {'exchange': self.exchange, 'currency': self.currency, 'symbol': self.currency, 'account': self.account, 'balance': self.balance, 'cw_balance': self.cw_balance, 'reserved': self.reserved, 'changed': self.changed, 'timestamp': self.timestamp}
        else:
            data = {'exchange': self.exchange, 'currency': self.currency, 'symbol': self.currency, 'account': self.account, 'balance': numeric_type(self.balance), 'cw_balance': numeric_type(self.cw_balance), 'reserved': numeric_type(self.reserved), 'changed': self.changed, 'timestamp': self.timestamp}
        return data if not none_to else convert_none_values(data, none_to)        

    def __repr__(self):
        return f'exchange: {self.exchange} currency: {self.currency} account: {self.account} balance: {self.balance} cw_balance: {self.cw_balance} reserved: {self.reserved} changed: {self.changed} timestamp: {self.timestamp}'


cdef class Position:
    cdef readonly str exchange
    cdef readonly str symbol
    cdef readonly str id
    cdef readonly object position
    cdef readonly object entry_price
    cdef readonly object unrealised_pnl
    cdef readonly object timestamp
    cdef readonly object raw  # Can be dict or list

    def __init__(self, exchange, symbol, id, position, entry_price, unrealised_pnl, timestamp, raw=None):
        assert isinstance(position, Decimal)
        assert isinstance(entry_price, Decimal)
        assert unrealised_pnl is None or isinstance(unrealised_pnl, Decimal)
        assert timestamp is None or isinstance(timestamp, float)

        self.exchange = exchange
        self.symbol = symbol
        self.id = id
        self.position = position
        self.entry_price = entry_price
        self.unrealised_pnl = unrealised_pnl
        self.timestamp = timestamp
        self.raw = raw

    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'id': self.id, 'position': self.position, 'entry_price': self.entry_price, 'unrealised_pnl': self.unrealised_pnl, 'timestamp': self.timestamp}
        else:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'id': self.id, 'position': numeric_type(self.position), 'entry_price': numeric_type(self.entry_price), 'unrealised_pnl': numeric_type(self.unrealised_pnl), 'timestamp': self.timestamp}
        return data if not none_to else convert_none_values(data, none_to)

    def __repr__(self):
        return f'exchange: {self.exchange} symbol: {self.symbol} id: {self.id} position: {self.position} entry_price: {self.entry_price} unrealised_pnl: {self.unrealised_pnl} timestamp: {self.timestamp}'

    def __eq__(self, cmp):
        return self.exchange == cmp.exchange and self.symbol == cmp.symbol and self.id == cmp.id and self.position == cmp.position and self.entry_price == cmp.entry_price and self.unrealised_pnl == cmp.unrealised_pnl and self.timestamp == cmp.timestamp

    def __hash__(self):
        return hash(self.__repr__())


cdef class LoopyPosition(Position):
    cdef readonly str account
    cdef readonly str margin_type
    cdef readonly str side
    # amount = position
    cdef readonly object amount 
    cdef readonly object cum_pnl

    def __init__(self, exchange, symbol, account, margin_type, side, entry_price, amount, unrealised_pnl, cum_pnl, timestamp, id=None, raw=None):
        assert isinstance(amount, Decimal) 
        assert cum_pnl is None or isinstance(cum_pnl, Decimal)

        if id is None:
            proxy_id = convert_hash(account) 
        else:
            proxy_id = id 
            
        super().__init__(exchange=exchange, symbol=symbol, id=proxy_id, position=amount, entry_price=entry_price, unrealised_pnl=unrealised_pnl, timestamp=timestamp, raw=raw)

        self.account = convert_hash(account)
        self.margin_type = margin_type
        self.side = side
        self.amount = amount
        self.cum_pnl = cum_pnl
        
    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'account': self.account, 'id': self.id, 'margin_type': self.margin_type, 'side': self.side, 'entry_price': self.entry_price, 'amount': self.amount, 'unrealised_pnl': self.unrealised_pnl, 'cum_pnl': self.cum_pnl, 'timestamp': self.timestamp}
        else:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'account': self.account, 'id': self.id, 'margin_type': self.margin_type, 'side': self.side, 'entry_price': numeric_type(self.entry_price), 'amount': numeric_type(self.amount), 'unrealised_pnl': numeric_type(self.unrealised_pnl), 'cum_pnl': numeric_type(self.cum_pnl), 'timestamp': self.timestamp}
        return data if not none_to else convert_none_values(data, none_to)

    def __repr__(self):
        return f'exchange: {self.exchange} symbol: {self.symbol} account: {self.account} id: {self.id} margin_type: {self.margin_type} side: {self.side} entry_price: {self.entry_price} amount: {self.amount} unrealised_pnl: {self.unrealised_pnl} cum_pnl: {self.cum_pnl} timestamp: {self.timestamp}'

cdef class LoopyTrade:
    cdef readonly str exchange
    cdef readonly str symbol
    cdef readonly object price
    cdef readonly object amount
    cdef readonly str side
    cdef readonly str id
    cdef readonly str type
    cdef readonly double timestamp
    cdef readonly object raw  # can be dict or list

    def __init__(self, exchange, symbol, side, amount, price, timestamp, id=None, type=None, raw=None):
        assert isinstance(price, Decimal)
        assert isinstance(amount, Decimal)

        self.exchange = exchange
        self.symbol = symbol
        self.side = side
        self.amount = amount
        self.price = price
        self.timestamp = timestamp
        self.id = id
        self.type = type
        self.raw = raw

    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'side': self.side, 'amount': self.amount, 'price': self.price, 'id': self.id, 'type': self.type, 'timestamp': self.timestamp}
        else:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'side': self.side, 'amount': numeric_type(self.amount), 'price': numeric_type(self.price), 'id': str(self.id), 'type': str(self.type), 'timestamp': self.timestamp}
        return data if not none_to else convert_none_values(data, none_to)

    def __repr__(self):
        return f"exchange: {self.exchange} symbol: {self.symbol} side: {self.side} amount: {self.amount} price: {self.price} id: {self.id} type: {self.type} timestamp: {self.timestamp}"

    def __eq__(self, cmp):
        return self.exchange == cmp.exchange and self.symbol == cmp.symbol and self.price == cmp.price and self.amount == cmp.amount and self.side == cmp.side and self.id == cmp.id and self.timestamp == cmp.timestamp

    def __hash__(self):
        return hash(self.__repr__())