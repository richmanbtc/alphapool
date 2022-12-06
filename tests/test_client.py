from unittest import TestCase, mock
import dataset
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import uuid
from alphapool import Client


class TestClient(TestCase):
    def setUp(self):
        # self.db = dataset.connect("sqlite:///:memory:")
        self.db = dataset.connect('postgresql://postgres:password@postgres:5432/postgres')

        self.client = Client(self.db, tournament=str(uuid.uuid4())[:16])

    @mock.patch('time.time', mock.MagicMock(return_value=pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()))
    def test_submit(self):
        self.client.submit(
            timestamp=int(pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()),
            model_id="model1",
            positions={
                "btc": 0.5,
                "eth": -0.5,
            },
        )
        self.client.submit(
            timestamp=int(pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()),
            model_id="model2",
            positions={
                "btc": 0.5,
                "xrp": -0.5,
            },
        )
        df = self.client.get_positions()
        expected = pd.DataFrame(
            [
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "model1",
                    "exchange": None,
                    "delay": 0.0,
                    "positions": {
                        "btc": 0.5,
                        "eth": -0.5,
                    },
                    "weights": {},
                    "orders": {},
                },
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "model2",
                    "exchange": None,
                    "delay": 0.0,
                    "positions": {
                        "btc": 0.5,
                        "xrp": -0.5,
                    },
                    "weights": {},
                    "orders": {}
                },
            ]
        ).set_index(["timestamp", "model_id"])
        assert_frame_equal(df, expected)

    @mock.patch('time.time', mock.MagicMock(return_value=pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()))
    def test_submit_weights(self):
        self.client.submit(
            timestamp=int(pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()),
            model_id="pf-model",
            weights={
                "model1": 0.7,
                "model2": 0.3,
            },
        )
        self.client.submit(
            timestamp=int(pd.to_datetime("2020/01/01 01:00:00", utc=True).timestamp()),
            model_id="pf-model",
            weights={
                "model1": 0.3,
                "model2": 0.7,
            },
        )
        df = self.client.get_positions()
        expected = pd.DataFrame(
            [
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "pf-model",
                    "exchange": None,
                    "delay": 0.0,
                    "positions": {},
                    "weights": {
                        "model1": 0.7,
                        "model2": 0.3,
                    },
                    "orders": {},
                },
                {
                    "timestamp": pd.to_datetime("2020/01/01 01:00:00", utc=True),
                    "model_id": "pf-model",
                    "exchange": None,
                    "delay": -3600.0,
                    "positions": {},
                    "weights": {
                        "model1": 0.3,
                        "model2": 0.7,
                    },
                    "orders": {}
                },
            ]
        ).set_index(["timestamp", "model_id"])
        assert_frame_equal(df, expected)

    @mock.patch('time.time', mock.MagicMock(return_value=pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()))
    def test_submit_orders(self):
        self.client.submit(
            timestamp=int(pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()),
            model_id="model1",
            exchange="exchange1",
            orders={
                "btc": [
                    {
                        "price": 1,
                        "amount": 2,
                        "duration": 3,
                        "is_buy": False,
                    }
                ],
            },
        )
        self.client.submit(
            timestamp=int(pd.to_datetime("2020/01/01 01:00:00", utc=True).timestamp()),
            model_id="model1",
            exchange="exchange1",
            orders={
                "btc": [
                    {
                        "price": 4,
                        "amount": 5,
                        "duration": 6,
                        "is_buy": True,
                    }
                ],
            },
        )

        df = self.client.get_positions()
        expected = pd.DataFrame(
            [
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "model1",
                    "exchange": "exchange1",
                    "delay": 0.0,
                    "positions": {},
                    "weights": {},
                    "orders": {
                        "btc": [
                            {
                                "price": 1,
                                "amount": 2,
                                "duration": 3,
                                "is_buy": False,
                            }
                        ],
                    }
                },
                {
                    "timestamp": pd.to_datetime("2020/01/01 01:00:00", utc=True),
                    "model_id": "model1",
                    "exchange": "exchange1",
                    "delay": -3600.0,
                    "positions": {},
                    "weights": {},
                    "orders": {
                        "btc": [
                            {
                                "price": 4,
                                "amount": 5,
                                "duration": 6,
                                "is_buy": True,
                            }
                        ],
                    }
                },
            ]
        ).set_index(["timestamp", "model_id"])
        assert_frame_equal(df, expected)

    @mock.patch('time.time', mock.MagicMock(return_value=pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()))
    def test_min_timestamp(self):
        self.client.submit(
            timestamp=int(pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp()),
            model_id="model1",
            positions={
                "btc": 0.5,
                "eth": -0.5,
            },
        )
        df = self.client.get_positions(min_timestamp=pd.to_datetime("2020/01/01 00:00:00", utc=True).timestamp())
        expected = pd.DataFrame([
            {
                "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                "model_id": "model1",
                "exchange": None,
                "delay": 0.0,
                "positions": {
                    "btc": 0.5,
                    "eth": -0.5,
                },
                "weights": {},
                "orders": {},
            },
        ]).set_index(["timestamp", "model_id"])
        assert_frame_equal(df, expected)

        df = self.client.get_positions(min_timestamp=pd.to_datetime("2020/01/01 00:00:01", utc=True).timestamp())
        expected = pd.DataFrame([
            {
                "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                "model_id": "model1",
                "exchange": None,
                "delay": 0.0,
                "positions": {
                    "btc": 0.5,
                    "eth": -0.5,
                },
                "weights": {},
                "orders": {},
            },
        ]).set_index(["timestamp", "model_id"]).iloc[:0]
        assert_frame_equal(df, expected)
