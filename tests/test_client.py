import json
import uuid
from contextlib import closing
from pathlib import Path
from unittest import TestCase, mock

import pandas as pd
import psycopg2
from pandas.testing import assert_frame_equal
from psycopg2 import sql

from alphapool import Client

def expected_positions(rows):
    records = [dict(exchange=None, delay=0.0, positions={}, weights={}, orders={})
               | row for row in rows]
    return pd.DataFrame(records, columns=[
        'timestamp', 'model_id', 'exchange', 'delay', 'positions', 'weights', 'orders',
    ]).set_index(['timestamp', 'model_id'])


class TestClientPostgres(TestCase):
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
        expected = expected_positions([
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "model1",
                    "positions": {
                        "btc": 0.5,
                        "eth": -0.5,
                    },
                },
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "model2",
                    "positions": {
                        "btc": 0.5,
                        "xrp": -0.5,
                    },
                },
            ])
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
        expected = expected_positions([
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "pf-model",
                    "weights": {
                        "model1": 0.7,
                        "model2": 0.3,
                    },
                },
                {
                    "timestamp": pd.to_datetime("2020/01/01 01:00:00", utc=True),
                    "model_id": "pf-model",
                    "delay": -3600.0,
                    "weights": {
                        "model1": 0.3,
                        "model2": 0.7,
                    },
                },
            ])
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
        expected = expected_positions([
                {
                    "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                    "model_id": "model1",
                    "exchange": "exchange1",
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
            ])
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
        expected = expected_positions([
            {
                "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                "model_id": "model1",
                "positions": {
                    "btc": 0.5,
                    "eth": -0.5,
                },
            },
        ])
        assert_frame_equal(df, expected)

        df = self.client.get_positions(min_timestamp=pd.to_datetime("2020/01/01 00:00:01", utc=True).timestamp())
        expected = expected_positions([
            {
                "timestamp": pd.to_datetime("2020/01/01 00:00:00", utc=True),
                "model_id": "model1",
                "positions": {
                    "btc": 0.5,
                    "eth": -0.5,
                },
            },
        ]).iloc[:0]
        assert_frame_equal(df, expected)


    def test_missing_tournament_table_is_not_created(self):
        client = Client(self.conn, tournament='missing')
        with self.assertRaises(psycopg2.errors.UndefinedTable):
            client.get_positions()

    def test_caller_rolls_back_after_failed_insert(self):
        timestamp = int(pd.Timestamp.now(tz='UTC').timestamp())
        self.client.submit(timestamp, 'example', positions={'btc': 0.5})
        self.conn.commit()
        with self.assertRaises(psycopg2.IntegrityError):
            self.client.submit(timestamp, 'example', positions={'btc': 0.7})
        self.assertEqual(self.conn.get_transaction_status(),
                         psycopg2.extensions.TRANSACTION_STATUS_INERROR)
        self.conn.rollback()
        self.client.submit(timestamp + 1, 'example', positions={'btc': 0.9})
        self.assertEqual(len(self.client.get_positions()), 2)

    def test_values_are_bound_as_parameters(self):
        timestamp = int(pd.Timestamp.now(tz='UTC').timestamp())
        model_id = "example'); DROP TABLE positions; --"
        self.client.submit(timestamp, model_id, positions={'btc': 0.5})
        result = self.client.get_positions()
        self.assertEqual(result.index.get_level_values('model_id').tolist(), [model_id])

    def test_caller_controls_commit_and_rollback(self):
        timestamp = int(pd.Timestamp.now(tz='UTC').timestamp())
        self.client.submit(timestamp, 'example')
        with closing(psycopg2.connect(dbname=self.database)) as observer:
            with observer.cursor() as cursor:
                cursor.execute('SELECT count(*) FROM positions')
                self.assertEqual(cursor.fetchone()[0], 0)
                self.conn.commit()
                cursor.execute('SELECT count(*) FROM positions')
                self.assertEqual(cursor.fetchone()[0], 1)
                self.client.submit(timestamp + 1, 'example')
                self.conn.rollback()
                cursor.execute('SELECT count(*) FROM positions')
                self.assertEqual(cursor.fetchone()[0], 1)


class TestClientUnit(TestCase):
    def test_init_does_not_execute_sql(self):
        conn = mock.MagicMock()
        Client(conn)
        conn.cursor.assert_not_called()

    def test_invalid_submissions_do_not_execute_sql(self):
        timestamp = int(pd.Timestamp.now(tz='UTC').timestamp())
        cases = [
            ({'timestamp': 0}, 'validation failed'),
            ({'model_id': ''}, 'validation failed'),
            ({'positions': {'btc': 101.0}}, 'validation failed'),
            ({'weights': {'example': 0.5}}, 'weights cannot be specified'),
            ({'model_id': 'pf-example', 'positions': {'btc': 0.5}},
             'positions cannot be specified'),
        ]
        for overrides, message in cases:
            with self.subTest(overrides=overrides):
                conn = mock.MagicMock()
                data = dict(timestamp=timestamp, model_id='example') | overrides
                with self.assertRaisesRegex(Exception, message):
                    Client(conn).submit(**data)
                conn.cursor.assert_not_called()

    @mock.patch('time.time', return_value=100000.0)
    def test_validation_boundaries(self, clock):
        order = dict(price=1, amount=100, duration=86400, is_buy=False)
        invalid = [
            {'timestamp': v} for v in (0, -1, 100000.0, 13599, 186401)
        ] + [
            {'model_id': v} for v in ('', None, 1)
        ] + [
            {'exchange': v} for v in ('', 1, False)
        ]
        for field in ('positions', 'weights'):
            invalid += [{field: v} for v in ([], '', {'': 1}, {1: 1},
                         {'btc': None}, {'btc': '1'}, {'btc': -101}, {'btc': 101},
                         {'btc': float('inf')}, {'btc': -float('inf')})]
        invalid += [{'orders': v, 'exchange': 'example'} for v in (
            {'': [order]}, {1: [order]}, {'btc': []}, {'btc': order},
            {'btc': [None]}, {'btc': [order | {'extra': 1}]},
        )]
        for field, values in {
            'price': (0, -1, '1', None), 'amount': (0, -1, 101, '1', None),
            'duration': (0, 86401, 1.0, '1', None), 'is_buy': (0, 1, 'true', None),
        }.items():
            invalid += [{'orders': {'btc': [order | {field: v}]}, 'exchange': 'example'}
                        for v in values]
            invalid.append({'orders': {'btc': [{k: v for k, v in order.items() if k != field}]},
                            'exchange': 'example'})
        for overrides in invalid:
            with self.subTest(overrides=overrides):
                conn = mock.MagicMock()
                with self.assertRaisesRegex(Exception, 'validation failed'):
                    Client(conn).submit(**(dict(timestamp=100000, model_id='example') | overrides))
                conn.cursor.assert_not_called()
        with self.assertRaisesRegex(Exception, 'exchange required'):
            Client(mock.MagicMock()).submit(100000, 'example', orders={'btc': [order]})

    @mock.patch('time.time', return_value=100000.0)
    def test_valid_submissions_and_normalization(self, clock):
        order = dict(price=1, amount=100, duration=86400, is_buy=False)
        cases = [
            {}, {'timestamp': 13600}, {'timestamp': 186400},
            {'positions': None, 'weights': None},
            {'positions': {'btc': -100, 'eth': 100.0}},
            {'model_id': 'pf-example', 'weights': {'example': -100}},
            {'orders': {}}, {'orders': []},
            {'exchange': 'example', 'orders': {'btc': [order]}},
            {'exchange': 'example', 'orders': {'btc': (order | {'duration': 1},)}},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides):
                conn = mock.MagicMock()
                data = dict(timestamp=100000, model_id='example') | overrides
                Client(conn).submit(**data)
                cursor = conn.cursor.return_value
                cursor.execute.assert_called_once()
                values = cursor.execute.call_args.args[1]
                for index, field in ((2, 'positions'), (3, 'weights')):
                    self.assertEqual(json.loads(values[index]), data.get(field) or {})
                if data.get('orders'):
                    self.assertEqual(json.loads(values[-1]),
                                     json.loads(json.dumps(data['orders'])))
                cursor.close.assert_called_once()
                conn.commit.assert_not_called()
                conn.rollback.assert_not_called()

    def test_tournament_table_is_quoted_without_creating_it(self):
        conn = mock.MagicMock()
        cursor = conn.cursor.return_value
        cursor.description = [(name,) for name in
                              ('timestamp', 'model_id', 'exchange', 'delay',
                               'positions', 'weights', 'orders')]
        cursor.fetchall.return_value = []
        client = Client(conn, tournament='example"table%name')
        conn.cursor.assert_not_called()
        client.get_positions()
        query, parameters = cursor.execute.call_args.args
        self.assertIn('example""table%%name', query)
        self.assertIn('timestamp >= %s', query)
        self.assertEqual(parameters, (0,))
        cursor.close.assert_called_once()

    def test_native_json_rows(self):
        conn = mock.MagicMock()
        cursor = conn.cursor.return_value
        columns = ('timestamp', 'model_id', 'exchange', 'delay', 'positions', 'weights', 'orders')
        cursor.description = [(name,) for name in columns]
        cursor.fetchall.return_value = [(1, 'example', None, 0.0, {'btc': 0.5}, {}, None)]
        client = Client(conn)
        result = client.get_positions(1)
        query, parameters = cursor.execute.call_args.args
        self.assertIn('timestamp >= %s', query)
        self.assertEqual(parameters, (1,))
        self.assertEqual(result.iloc[0]['positions'], {'btc': 0.5})
        self.assertEqual(result.iloc[0]['orders'], {})
        cursor.close.assert_called_once()
