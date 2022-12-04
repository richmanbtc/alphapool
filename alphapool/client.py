from cerberus import Validator
import pandas as pd
import time


class Client:
    def __init__(self, db, tournament=None):
        self._db = db
        table_name = 'positions' if tournament is None else '{}_positions'.format(tournament)
        self._table = db.create_table(table_name)

        self._table.create_column('timestamp', db.types.guess(1))
        self._table.create_column('model_id', db.types.guess('model'))
        self._table.create_column('exchange', db.types.guess('exchange'))
        self._table.create_column('delay', db.types.guess(1.2))
        self._table.create_column('positions', db.types.guess({ 'btc': 1.0 }))
        self._table.create_column('weights', db.types.guess({ 'model': 1.0 }))
        self._table.create_column('orders', db.types.guess({ 'btc': [] }))

        # remove old
        # self._table.drop_column('tournament')

        self._table.create_index(['timestamp', 'model_id'], unique=True)

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
                                "isBuy": {
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

        self._table.insert(data)

    def get_positions(self, min_timestamp=0):
        results = self._table.find(
            timestamp={ 'gte': min_timestamp },
        )
        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, unit="s")
        df = df.drop(columns=['id'])
        df['orders'] = df['orders'].apply(lambda x: {} if pd.isnull(x) else x)

        return df.set_index(["timestamp", "model_id"]).sort_index()


def _normalize_dict(x):
    if x is None:
        return {}
    return x
