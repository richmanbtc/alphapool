import json

from collections.abc import Mapping, Sequence
import pandas as pd
import time


class Client:
    def __init__(self, conn, tournament=None):
        """Use a PostgreSQL DB-API connection and an already migrated table.

        The caller owns the connection and manages commits and rollbacks.
        """
        self._conn = conn
        table_name = 'positions' if tournament is None else '{}_positions'.format(tournament)
        self._table_name = '"' + table_name.replace('"', '""').replace('%', '%%') + '"'

    def submit(self, timestamp, model_id, positions={}, weights={}, orders=None, exchange=None):
        data = dict(
            timestamp=timestamp,
            model_id=model_id,
            positions=positions,
            weights=weights,
            delay=time.time() - timestamp,
        )
        if exchange is not None:
            data['exchange'] = exchange
        if orders is not None and len(orders) > 0:
            data['orders'] = orders
            if exchange is None:
                raise Exception('exchange required when submit orders')
        if not _valid_submission(data):
            raise Exception("validation failed {}".format(data))
        for field in ('positions', 'weights'):
            if data[field] is None:
                data[field] = {}

        is_portfolio = model_id.startswith("pf-")
        if is_portfolio:
            if len(data["positions"]) > 0:
                raise Exception("positions cannot be specified for portfolio")
        else:
            if len(data["weights"]) > 0:
                raise Exception("weights cannot be specified for non portfolio")

        columns = list(data)
        values = [json.dumps(data[column]) if column in {'positions', 'weights', 'orders'}
                  else data[column] for column in columns]
        placeholders = ", ".join(["%s"] * len(values))
        column_names = ', '.join('"' + column + '"' for column in columns)
        cursor = self._conn.cursor()
        try:
            cursor.execute(
                f'INSERT INTO {self._table_name} ({column_names}) VALUES ({placeholders})',
                values,
            )
        finally:
            cursor.close()

    def get_positions(self, min_timestamp=0):
        cursor = self._conn.cursor()
        try:
            cursor.execute(
                'SELECT timestamp, model_id, exchange, delay, positions, weights, orders '
                f'FROM {self._table_name} WHERE timestamp >= %s',
                (min_timestamp,),
            )
            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
        for result in results:
            for column in ('positions', 'weights', 'orders'):
                value = result[column]
                if isinstance(value, (str, bytes, bytearray)):
                    result[column] = json.loads(value)
        if len(results) == 0:
            return pd.DataFrame([
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "model1",
                    "exchange": "exchange1",
                    "delay": 0.0,
                    "positions": {},
                    "weights": {},
                    "orders": {},
                }
            ]).set_index(["timestamp", "model_id"]).iloc[:0]
        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, unit="s")
        df['orders'] = df['orders'].apply(lambda x: {} if pd.isnull(x) else x)

        return df.set_index(["timestamp", "model_id"]).sort_index()


def _number(value, minimum, maximum=float('inf'), integer=False):
    return (isinstance(value, int if integer else (int, float))
            and not (value < minimum or value > maximum))


def _named_mapping(value):
    return isinstance(value, Mapping) and all(isinstance(k, str) and k for k in value)


def _valid_submission(data):
    if not (_number(data['timestamp'], 1, integer=True)
            and isinstance(data['model_id'], str) and data['model_id']
            and _number(data['delay'], -86400, 86400)):
        return False
    if 'exchange' in data and not (isinstance(data['exchange'], str) and data['exchange']):
        return False
    for field in ('positions', 'weights'):
        value = data[field]
        if value is not None and not (_named_mapping(value)
                                      and all(_number(v, -100, 100) for v in value.values())):
            return False
    if 'orders' not in data:
        return True
    orders = data['orders']
    if not (_named_mapping(orders) and orders):
        return False
    for items in orders.values():
        if not (isinstance(items, Sequence) and not isinstance(items, str) and items):
            return False
        for order in items:
            if not (isinstance(order, Mapping)
                    and order.keys() == {'price', 'amount', 'duration', 'is_buy'}
                    and _number(order['price'], 0) and order['price'] != 0
                    and _number(order['amount'], 0, 100) and order['amount'] != 0
                    and _number(order['duration'], 1, 86400, integer=True)
                    and isinstance(order['is_buy'], bool)):
                return False
    return True
