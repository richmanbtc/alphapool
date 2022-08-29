from unittest import TestCase
import dataset
from .helper import test_submit
from alphapool import Client


class TestClient(TestCase):
    def setUp(self):
        self.db = dataset.connect('sqlite:///:memory:')
        self.client = Client(self.db)

    def test_submit(self):
        test_submit(self, self.client)
