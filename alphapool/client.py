from cerberus import Validator
import pandas as pd
import time
from pandas.api.types import is_numeric_dtype


class Client:
    def __init__(self, db):
        self._db = db
        self._table = db.create_table('positions')

    def submit(self, tournament, timestamp, model_id, positions={}, weights={}):
        v = Validator({
            'tournament': {
                'type': 'string', 'empty': False, 'required': True
            },
            'timestamp': {
                'type': 'integer', 'min': 1, 'empty': False, 'required': True
            },
            'model_id': {
                'type': 'string', 'empty': False, 'required': True
            },
            'positions': {
                'type': 'dict',
                'keysrules': {'type': 'string', 'empty': False },
                'valuesrules': {'type': 'float', 'empty': False, 'min': -100, 'max': 100 },
            },
            'weights': {
                'type': 'dict',
                'keysrules': {'type': 'string', 'empty': False },
                'valuesrules': {'type': 'float', 'empty': False, 'min': 0, 'max': 1 },
            },
            'delay': {
                'type': 'float', 'empty': False, 'required': True
            },
        })
        data = dict(
            tournament=tournament,
            timestamp=timestamp,
            model_id=model_id,
            positions=positions,
            weights=weights,
            delay=time.time() - timestamp
        )
        if not v.validate(data):
            raise Exception("validation failed {}".format(data))
        if len(data['positions']) > 0 and len(data['weights']) > 0:
            raise Exception('positions and weights cannot be specified simultaneously')
        self._table.upsert(data, ['tournament', 'timestamp', 'model_id'])

    def get_positions(self, tournament):
        results = self._table.find(tournament=tournament)
        df = pd.DataFrame(results)

        dfs = [df.drop(['id', 'tournament', 'positions', 'weights'], axis=1)]

        def add_flattened(prefix, col):
            df_flattened = pd.json_normalize(df[col])
            df_flattened.columns = prefix + df_flattened.columns
            if is_numeric_dtype(df_flattened.iloc[:, 0]):
                dfs.append(df_flattened)

        add_flattened('p.', 'positions')
        add_flattened('w.', 'weights')

        df = pd.concat(dfs, axis=1)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, unit='s')
        return df.set_index(['model_id', 'timestamp']).sort_index()