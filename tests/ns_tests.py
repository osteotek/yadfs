import unittest
from name_server import NameServer


class NSTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ns = NameServer()

    def test_put_and_read_file(self):
        r = self.ns.create_file({'path': '/var/some_dir/file', 'size': 1042, 'chunks': {'file_01': 'cs-1'}})
        self.assertEquals('ok', r)

        d = self.ns.get_file_info('/var/some_dir/file')
        self.assertEquals(1042, d['size'])
        self.assertEquals(d['chunks']['file_01']['cs'], 'cs-1')
        self.assertEquals(d['chunks']['file_01']['path'], '/var/some_dir/file_01')

if __name__ == '__main__':
    unittest.main()
