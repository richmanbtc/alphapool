from cerberus import Validator
import pandas as pd
import time


class Client:
    def __init__(self, db):
        self._db = db
        self._table = db.create_table('positions')

    def submit(self, tournament, timestamp, model_id, positions):
        v = Validator({
            'tournament': {
                'type': 'string', 'empty': False
            },
            'timestamp': {
                'type': 'integer', 'min': 1, 'empty': False
            },
            'model_id': {
                'type': 'string', 'empty': False
            },
            'positions': {
                'type': 'dict',
                'keysrules': {'type': 'string', 'empty': False },
                'valuesrules': {'type': 'float', 'empty': False, 'min': -1, 'max': 1 },
                'empty': False
            },
            'delay': {
                'type': 'float', 'empty': False
            },
        })
        data = dict(
            tournament=tournament,
            timestamp=timestamp,
            model_id=model_id,
            positions=positions,
            delay=time.time() - timestamp
        )
        if not v.validate(data):
            raise Exception("validation failed {}".format(data))
        self._table.upsert(data, ['tournament', 'timestamp', 'model_id'])

    def get_positions(self, tournament):
        results = self._table.find(tournament=tournament)
        df = pd.DataFrame(results)
        df2 = pd.json_normalize(df['positions'])
        df2.columns = 'p.' + df2.columns
        df = pd.concat([
            df.drop(['id', 'tournament', 'positions'], axis=1),
            df2
        ], axis=1)
        df = df.fillna(0)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, unit='s')
        return df.set_index(['model_id', 'timestamp']).sort_index()
