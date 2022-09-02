from cerberus import Validator
import pandas as pd
import time
from pandas.api.types import is_numeric_dtype


class Client:
    def __init__(self, db):
        self._db = db

    def submit(self, tournament, timestamp, model_id, positions={}, weights={}):
        v = Validator(
            {
                "tournament": {"type": "string", "empty": False, "required": True},
                "timestamp": {
                    "type": "integer",
                    "min": 1,
                    "empty": False,
                    "required": True,
                },
                "model_id": {"type": "string", "empty": False, "required": True},
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
                        "min": 0,
                        "max": 1,
                    },
                    "coerce": _normalize_dict,
                    "required": True,
                },
                "delay": {"type": "float", "empty": False, "required": True},
            }
        )
        row = dict(
            tournament=tournament,
            timestamp=timestamp,
            model_id=model_id,
            positions=positions,
            weights=weights,
            delay=time.time() - timestamp,
        )
        if not v.validate(row):
            raise Exception("validation failed {}".format(row))
        row = v.document

        is_portfolio = model_id.startswith("pf-")
        if is_portfolio:
            if len(row["positions"]) > 0:
                raise Exception("positions cannot be specified for portfolio")
        else:
            if len(row["weights"]) > 0:
                raise Exception("weights cannot be specified for non portfolio")

        self._get_positions_table(create=True).upsert(row, ["tournament", "timestamp", "model_id"])

    def get_positions(self, tournament):
        results = self._get_positions_table().find(tournament=tournament)
        df = pd.DataFrame(results)

        dfs = [df[["model_id", "timestamp", "delay"]]]

        def add_flattened(prefix, col):
            if col not in df.columns:
                return
            df_flattened = pd.json_normalize(df[col].where(~df[col].isna(), {}))
            df_flattened.columns = prefix + df_flattened.columns
            if df_flattened.shape[1] > 0 and is_numeric_dtype(df_flattened.iloc[:, 0]):
                dfs.append(df_flattened)

        add_flattened("p.", "positions")
        add_flattened("w.", "weights")

        df = pd.concat(dfs, axis=1)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, unit="s")
        return df.set_index(["model_id", "timestamp"]).sort_index()

    def set_cache(self, model_id, version, data={}):
        v = Validator(
            {
                "model_id": {"type": "string", "empty": False, "required": True},
                "version": {"type": "string", "empty": False, "required": True},
                "data": {
                    "type": "dict",
                    "required": True,
                },
            }
        )
        row = dict(
            model_id=model_id,
            version=version,
            data=data,
        )
        if not v.validate(row):
            raise Exception("validation failed {}".format(row))
        row = v.document

        with self._db:
            table = self._get_caches_table(create=True)
            if 'model_id' in table.columns:
                table.delete(model_id=model_id)
            table.insert(row)

    def get_cache(self, model_id, version):
        table = self._get_caches_table()
        if table is None:
            return None
        result = table.find_one(model_id=model_id, version=version)
        if result is None:
            return None
        else:
            return result['data']

    def _get_positions_table(self, create=False):
        return self._get_table("positions", create=create)

    def _get_caches_table(self, create=False):
        return self._get_table("caches", create=create)

    def _get_table(self, name, create=False):
        if create:
            return self._db.create_table(name)
        else:
            if name in self._db.tables:
                return self._db.load_table(name)
        return None


def _normalize_dict(x):
    if x is None:
        return {}
    return x
