import json

from cerberus import Validator
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
        v = Validator(
            {
                "timestamp": {
                    "type": "integer",
                    "min": 1,
                    "empty": False,
                    "required": True,
                },
                "model_id": {"type": "string", "empty": False, "required": True},
                "exchange": {"type": "string", "empty": False, "required": False},
                "positions": {
                    "type": "dict",
                    "keysrules": {"type": "string", "empty": False},
                    "valuesrules": {
                        "type": "float",
                        "empty": False,
                        "min": -100,
                        "max": 100,
                    },
                    "coerce": _normalize_dict,
                    "required": True,
                },
                "weights": {
                    "type": "dict",
                    "keysrules": {"type": "string", "empty": False},
                    "valuesrules": {
                        "type": "float",
                        "empty": False,
                        "min": -100,
                        "max": 100,
                    },
                    "coerce": _normalize_dict,
                    "required": True,
                },
                "orders": {
                    "type": "dict",
                    "keysrules": {"type": "string", "empty": False},
                    "valuesrules": {
                        "type": "list",
                        "schema": {
                            "type": "dict",
                            "schema": {
                                "price": {
                                    "type": "float",
                                    "required": True,
                                    "min": 0,
                                    "forbidden": [0],
                                },
                                "amount": {
                                    "type": "float",
                                    "required": True,
                                    "min": 0,
                                    "max": 100,
                                    "forbidden": [0],
                                },
                                "duration": {
                                    "type": "integer",
                                    "min": 1,
                                    "max": 24 * 60 * 60,
                                    "required": True,
                                },
                                "is_buy": {
                                    "type": "boolean",
                                    'required': True,
                                }
                            },
                        },
                        "empty": False
                    },
                    "empty": False,
                },
                "delay": {
                    "type": "float",
                    "min": -24 * 60 * 60,
                    "max": 24 * 60 * 60,
                    "empty": False,
                    "required": True,
                },
            }
        )
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
        if not v.validate(data):
            raise Exception("validation failed {}".format(data))
        data = v.document

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



def _normalize_dict(x):
    if x is None:
        return {}
    return x
