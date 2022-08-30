from unittest import TestCase
import dataset
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from alphapool import Client


class TestClient(TestCase):
    def setUp(self):
        self.db = dataset.connect('sqlite:///:memory:')
        self.client = Client(self.db)

    def test_submit(self):
        self.client.submit(
            tournament='crypto_v1',
            timestamp=int(pd.to_datetime('2020/01/01 00:00:00', utc=True).timestamp()),
            model_id='model1',
            positions={
                'btc': 0.5,
                'eth': -0.5,
            }
        )
        self.client.submit(
            tournament='crypto_v1',
            timestamp=int(pd.to_datetime('2020/01/01 00:00:00', utc=True).timestamp()),
            model_id='model2',
            positions={
                'btc': 0.5,
                'xrp': -0.5,
            }
        )
        df = self.client.get_positions('crypto_v1')
        expected = pd.DataFrame([
            {
                'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.5,
                'p.eth': -0.5,
                'p.xrp': np.nan,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
                'model_id': 'model2',
                'p.btc': 0.5,
                'p.eth': np.nan,
                'p.xrp': -0.5,
            },
        ]).set_index(['model_id', 'timestamp'])
        assert_frame_equal(df.drop('delay', axis=1), expected)

    def test_submit_weights(self):
        self.client.submit(
            tournament='crypto_v1',
            timestamp=int(pd.to_datetime('2020/01/01 00:00:00', utc=True).timestamp()),
            model_id='portfolio',
            weights={
                'model1': 0.7,
                'model2': 0.3,
            }
        )
        self.client.submit(
            tournament='crypto_v1',
            timestamp=int(pd.to_datetime('2020/01/01 01:00:00', utc=True).timestamp()),
            model_id='portfolio',
            weights={
                'model1': 0.3,
                'model2': 0.7,
            }
        )
        df = self.client.get_positions('crypto_v1')
        expected = pd.DataFrame([
            {
                'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
                'model_id': 'portfolio',
                'w.model1': 0.7,
                'w.model2': 0.3,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 01:00:00', utc=True),
                'model_id': 'portfolio',
                'w.model1': 0.3,
                'w.model2': 0.7,
            },
        ]).set_index(['model_id', 'timestamp'])
        assert_frame_equal(df.drop('delay', axis=1), expected)
