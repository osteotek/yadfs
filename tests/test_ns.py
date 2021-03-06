import unittest
from utils.enums import Status, NodeType
from ns.name_server import NameServer


class NSTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ns = NameServer(dump_on=False)

    def test_put_and_read_file(self):
        r = self.ns.create_file({'path': '/var/some_dir/file', 'size': 1042, 'chunks': {'/var/some_dir/file_01': 'cs-1'}})
        self.assertEqual(Status.ok, r['status'])

        d = self.ns.get_file_info('/var/some_dir/file')
        self.assertEqual(Status.ok, d['status'])
        self.assertEqual(1042, d['size'])
        self.assertEqual(d['chunks']['/var/some_dir/file_01'], 'cs-1')

    def test_list_directory(self):
        self.ns.create_file({'path': '/var/some_dir/file1', 'size': 1042, 'chunks': {'file1_01': 'cs-1'}})
        self.ns.create_file({'path': '/var/some_dir/file2', 'size': 2122, 'chunks': {'file2_01': 'cs-1'}})
        self.ns.create_file({'path': '/var/some_dir/usr/txt', 'size': 2122, 'chunks': {'txt_01': 'cs-1'}})

        r = self.ns.list_directory('/var/some_dir')

        for item, info in r['items'].items():
            info.pop('date', None)

        items = {"file1": {'path': '/var/some_dir/file1', 'type': NodeType.file, 'size': 1042, 'chunks': {'file1_01': 'cs-1'}, 'status': 200},
                 'file2': {'path': '/var/some_dir/file2', 'type': NodeType.file, 'size': 2122, 'chunks': {'file2_01': 'cs-1'}, 'status': 200},
                 'usr': {'path': '/var/some_dir/usr', 'type': NodeType.directory, 'size': 2122, 'chunks': {}, 'status': 200}}

        self.assertEqual(Status.ok, r['status'])
        self.assertDictEqual(items, r['items'])

    def test_root_list(self):
        self.ns.create_file({'path': '/some_dir/file1', 'size': 1042, 'chunks': {'file1_01': 'cs-1'}})
        self.ns.create_file({'path': '/my_dir/file2', 'size': 2122, 'chunks': {'file2_01': 'cs-1'}})
        self.ns.create_file({'path': '/another_file', 'size': 2122, 'chunks': {'txt_01': 'cs-1'}})

        r = self.ns.list_directory('/')

        for item, info in r['items'].items():
            info.pop('date', None)

        items = {'some_dir': NodeType.directory, "my_dir": NodeType.directory, "another_file": NodeType.file}
        items = {"some_dir": {'path': '/some_dir', 'type': NodeType.directory, 'size': 1042,
                           'chunks': {}, 'status': 200},
                 'my_dir': {'path': '/my_dir', 'type': NodeType.directory, 'size': 2122,
                           'chunks': {}, 'status': 200},
                 'another_file': {'path': '/another_file', 'type': NodeType.file, 'size': 2122, 'chunks': {'txt_01': 'cs-1'},
                         'status': 200}}

        self.assertEqual(Status.ok, r['status'])
        self.assertDictEqual(items, r['items'])

    def test_list_empty_root(self):
        r = self.ns.list_directory('/')

        items = {}

        self.assertEqual(Status.ok, r['status'])
        self.assertDictEqual(items, r['items'])

    def test_make_directory(self):
        r = self.ns.make_directory('/my/dir/')
        self.assertEqual(Status.ok, r['status'])

        r = self.ns.make_directory('/my/dir/and/new/dir')
        self.assertEqual(Status.ok, r['status'])

        d = self.ns.get_file_info('/my/dir/and/new/dir')
        self.assertEqual(NodeType.directory, d['type'])

    def test_make_directory_with_error(self):
        self.ns.create_file({'path': '/my/dir/file', 'size': 0, 'chunks': {}})
        r = self.ns.make_directory('/my/dir/file/my_dir')
        self.assertEqual(Status.error, r['status'])
        r = self.ns.get_file_info('/my/dir/file/my_dir')
        self.assertEqual(Status.not_found, r['status'])

    def test_make_directory_already_exists(self):
        self.ns.make_directory('/my/dir/file')
        r = self.ns.make_directory('/my/dir/file')
        self.assertEqual(Status.already_exists, r['status'])

    def test_size_of(self):
        self.ns.create_file({'path': '/my/file', 'size': 100, 'chunks': {}})
        self.ns.create_file({'path': '/my/file2', 'size': 150, 'chunks': {}})

        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/subdir/file5', 'size': 250, 'chunks': {}})

        r = self.ns.size_of('/my')
        self.assertEqual(Status.ok, r['status'])
        self.assertEqual(100+150+200+250, r['size'])

    def test_size_of_not_found(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/subdir/file5', 'size': 250, 'chunks': {}})

        r = self.ns.size_of('/my/some/path')
        self.assertEqual(Status.not_found, r['status'])

    def test_create_file_exists(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        r = self.ns.create_file({'path': '/my/dir/file3', 'size': 250, 'chunks': {}})

        self.assertEqual(Status.already_exists, r['status'])

    def test_delete_file(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.delete('/my/dir/file3')

        r = self.ns.get_file_info('/my/dir/file3')
        self.assertEqual(Status.not_found, r['status'])

    def test_dump(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/another', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/dir3/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/dir5/file3', 'size': 200, 'chunks': {}})
        self.ns._dump()
        self.ns._load_dump()

        r = self.ns.get_file_info('/my/dir/another')
        self.assertEqual(Status.ok, r['status'])

    def test_get_cs(self):
        self.ns.heartbeat("localhost:9999")

        r = self.ns.get_cs('/var/something')
        self.assertEqual('localhost:9999', r['cs'])

    def test_check_status(self):
        self.ns.make_directory('/my/dir/file')
        self.ns.create_file({'path': '/some_dir/file1', 'size': 1042, 'chunks': {'file1_01': 'cs-1'}})

        fs = self.ns.get_file_info('/some_dir/file1')
        ds = self.ns.get_file_info('/my/dir/file')

        self.assertEqual(NodeType.file, fs['type'])
        self.assertEqual(NodeType.directory, ds['type'])

if __name__ == '__main__':
    unittest.main()
