from unittest import TestCase
import dataset
from alphapool import Client


class TestClientCaches(TestCase):
    def setUp(self):
        self.db = dataset.connect("sqlite:///:memory:")
        self.client = Client(self.db)

    def test_empty(self):
        self.assertEqual(self.client.get_cache("model1", 'v1'), None)

    def test_set_cache(self):
        self.client.set_cache(
            model_id="model1",
            version='v1',
            data={
                'a': 1,
            },
        )
        self.client.set_cache(
            model_id="model1",
            version='v2',
            data={
                'a': 1,
            },
        )
        self.assertEqual(self.client.get_cache("model1", 'v1'), None)
        self.assertEqual(self.client.get_cache("model1", 'v2'), {
            'a': 1
        })
        self.assertEqual(self.client.get_cache("model2", 'v2'), None)
