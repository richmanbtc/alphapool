from unittest import TestCase
import pandas as pd
from pandas.testing import assert_frame_equal
from alphapool import asfreq_positions, convert_to_executable_time


class TestUtils(TestCase):
    def test_asfreq_positions(self):
        input = pd.DataFrame([
            {
                'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.1,
                'p.eth': -0.1,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 01:05:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.2,
                'p.eth': -0.2,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 03:00:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.3,
                'p.eth': -0.3,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 03:00:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.4,
                'p.eth': -0.4,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
                'model_id': 'model2',
                'p.btc': 0.5,
                'p.eth': -0.5,
            }
        ]).set_index(['model_id', 'timestamp'])

        expected = pd.DataFrame([
            {
                'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.1,
                'p.eth': -0.1,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 01:00:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.1,
                'p.eth': -0.1,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 02:00:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.2,
                'p.eth': -0.2,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 03:00:00', utc=True),
                'model_id': 'model1',
                'p.btc': 0.4,
                'p.eth': -0.4,
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
                'model_id': 'model2',
                'p.btc': 0.5,
                'p.eth': -0.5,
            }
        ]).set_index(['model_id', 'timestamp'])

        assert_frame_equal(asfreq_positions(input, '1H'), expected)

    def test_convert_to_executable_time(self):
        input = pd.DataFrame([
            {
                'timestamp': pd.to_datetime('2020/01/01 01:00:00', utc=True),
                'model_id': 'model1',
                'delay': -10
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 02:00:00', utc=True),
                'model_id': 'model1',
                'delay': -1
            },
        ]).set_index(['model_id', 'timestamp'])

        expected = pd.DataFrame([
            {
                'timestamp': pd.to_datetime('2020/01/01 01:00:00', utc=True),
                'model_id': 'model1',
                'delay': -10
            },
            {
                'timestamp': pd.to_datetime('2020/01/01 02:00:04', utc=True),
                'model_id': 'model1',
                'delay': -1
            },
        ]).set_index(['model_id', 'timestamp'])

        assert_frame_equal(convert_to_executable_time(input, 5), expected)
