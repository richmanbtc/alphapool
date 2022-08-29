from unittest import TestCase
import subprocess
import dataset
from .helper import test_submit
from .wait_for_tcp_port import wait_for_port
from alphapool import Client


class TestOrbitClient(TestCase):
    def setUp(self):
        subprocess.Popen(["bash", "tests/launch_orbit_db_server.sh"])
        wait_for_port(port=3000, host='localhost', timeout=30.0)
        self.db = dataset.connect('sqlite:///:memory:')
        self.client = Client(self.db)

    def test_submit(self):
        test_submit(self, self.client)

