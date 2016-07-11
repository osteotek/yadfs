import unittest
import time
from utils.enums import Status, NodeType
from ns.name_server import NameServer


@unittest.skip("long test should be run manually")
class LongTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ns = NameServer(dump_on=False)

    def test_get_cs_after_timeout(self):
        self.ns.heartbeat("cs-22", "localhost:9999")
        time.sleep(2)

        r = self.ns.get_cs('/var/something')
        self.assertEquals(Status.not_found, r['status'])


if __name__ == '__main__':
    unittest.main()
