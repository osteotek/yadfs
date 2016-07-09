import unittest
from enums import Status, NodeType

from ns.name_server import NameServer


class NSTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ns = NameServer()

    def test_put_and_read_file(self):
        r = self.ns.create_file({'path': '/var/some_dir/file', 'size': 1042, 'chunks': {'file_01': 'cs-1'}})
        self.assertEqual(Status.ok, r['status'])

        d = self.ns.get_file_info('/var/some_dir/file')
        self.assertEqual(Status.ok, d['status'])
        self.assertEqual(1042, d['size'])
        self.assertEqual(d['chunks']['file_01']['cs'], 'cs-1')
        self.assertEqual(d['chunks']['file_01']['path'], '/var/some_dir/file_01')

    def test_list_directory(self):
        self.ns.create_file({'path': '/var/some_dir/file1', 'size': 1042, 'chunks': {'file1_01': 'cs-1'}})
        self.ns.create_file({'path': '/var/some_dir/file2', 'size': 2122, 'chunks': {'file2_01': 'cs-1'}})
        self.ns.create_file({'path': '/var/some_dir/usr/txt', 'size': 2122, 'chunks': {'txt_01': 'cs-1'}})

        r = self.ns.list_directory('/var/some_dir')

        items = [{'name': 'file1', 'type': NodeType.file},
                 {'name': 'file2', 'type': NodeType.file},
                 {'name': 'usr', 'type': NodeType.directory}]

        self.assertEqual(Status.ok, r['status'])
        self.assertListEqual(items, r['items'])


if __name__ == '__main__':
    unittest.main()
