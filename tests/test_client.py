from unittest import TestCase, mock
from contextlib import closing
from pathlib import Path
import uuid

import psycopg2
from psycopg2 import sql
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from alphapool import Client


class TestClient(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg2.connect()
        cls.admin.autocommit = True
        cls.addClassCleanup(cls.admin.close)
        cls.database = 'test_' + uuid.uuid4().hex
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.database)))
        cls.addClassCleanup(cls.drop_database)
        migrations = sorted((Path(__file__).resolve().parents[1] / 'migrations').glob('*.sql'))
        if not migrations:
            raise RuntimeError('No SQL migrations found')
        with closing(psycopg2.connect(dbname=cls.database)) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                for migration in migrations:
                    cursor.execute(migration.read_text(encoding='utf-8'))

    @classmethod
    def drop_database(cls):
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.database)))

    def setUp(self):
        self.conn = psycopg2.connect(dbname=self.database)
        self.addCleanup(self.conn.close)
        with self.conn.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE positions RESTART IDENTITY')
        self.conn.commit()
        self.client = Client(self.conn)

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


    def test_init_does_not_create_schema(self):
        conn = mock.MagicMock(wraps=self.conn)
        Client(conn, paramstyle=psycopg2.paramstyle)
        conn.cursor.assert_not_called()
        client = Client(self.conn, tournament='missing')
        with self.assertRaises(psycopg2.errors.UndefinedTable):
            client.get_positions()

    def test_duplicate_insert_rolls_back_and_connection_remains_usable(self):
        timestamp = int(pd.Timestamp.now(tz='UTC').timestamp())
        self.client.submit(timestamp, 'example', positions={'btc': 0.5})
        with self.assertRaises(psycopg2.IntegrityError):
            self.client.submit(timestamp, 'example', positions={'btc': 0.7})
        self.client.submit(timestamp + 1, 'example', positions={'btc': 0.9})
        self.assertEqual(self.conn.get_transaction_status(),
                         psycopg2.extensions.TRANSACTION_STATUS_IDLE)
        self.assertEqual(len(self.client.get_positions()), 2)

    def test_values_are_bound_as_parameters(self):
        timestamp = int(pd.Timestamp.now(tz='UTC').timestamp())
        model_id = "example'); DROP TABLE positions; --"
        self.client.submit(timestamp, model_id, positions={'btc': 0.5})
        result = self.client.get_positions()
        self.assertEqual(result.index.get_level_values('model_id').tolist(), [model_id])


    def test_tournament_table_is_quoted_without_creating_it(self):
        conn = mock.MagicMock()
        cursor = conn.cursor.return_value
        cursor.description = [(name,) for name in
                              ('timestamp', 'model_id', 'exchange', 'delay',
                               'positions', 'weights', 'orders')]
        cursor.fetchall.return_value = []
        client = Client(conn, tournament='example"table%name', paramstyle='format')
        conn.cursor.assert_not_called()
        client.get_positions()
        query, parameters = cursor.execute.call_args.args
        self.assertIn('example""table%%name', query)
        self.assertIn('timestamp >= %s', query)
        self.assertEqual(parameters, (0,))
        cursor.close.assert_called_once()


    def test_pyformat_with_native_json_rows(self):
        conn = mock.MagicMock()
        cursor = conn.cursor.return_value
        columns = ('timestamp', 'model_id', 'exchange', 'delay', 'positions', 'weights', 'orders')
        cursor.description = [(name,) for name in columns]
        cursor.fetchall.return_value = [(1, 'example', None, 0.0, {'btc': 0.5}, {}, None)]
        client = Client(conn, paramstyle='pyformat')
        result = client.get_positions(1)
        query, parameters = cursor.execute.call_args.args
        self.assertIn('timestamp >= %(p0)s', query)
        self.assertEqual(parameters, {'p0': 1})
        self.assertEqual(result.iloc[0]['positions'], {'btc': 0.5})
        self.assertEqual(result.iloc[0]['orders'], {})
        cursor.close.assert_called_once()
