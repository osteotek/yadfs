import unittest
from enums import Status, NodeType
from ns.name_server import NameServer


class NSTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ns = NameServer()

    def test_put_and_read_file(self):
        r = self.ns.create_file({'path': '/var/some_dir/file', 'size': 1042, 'chunks': {'file_01': 'cs-1'}})
        self.assertEquals(Status.ok, r['status'])

        d = self.ns.get_file_info('/var/some_dir/file')
        self.assertEquals(Status.ok, d['status'])
        self.assertEquals(1042, d['size'])
        self.assertEquals(d['chunks']['file_01']['cs'], 'cs-1')
        self.assertEquals(d['chunks']['file_01']['path'], '/var/some_dir/file_01')

    def test_list_directory(self):
        self.ns.create_file({'path': '/var/some_dir/file1', 'size': 1042, 'chunks': {'file1_01': 'cs-1'}})
        self.ns.create_file({'path': '/var/some_dir/file2', 'size': 2122, 'chunks': {'file2_01': 'cs-1'}})
        self.ns.create_file({'path': '/var/some_dir/usr/txt', 'size': 2122, 'chunks': {'txt_01': 'cs-1'}})

        r = self.ns.list_directory('/var/some_dir')

        items = [{'name': 'file1', 'type': NodeType.file},
                 {'name': 'file2', 'type': NodeType.file},
                 {'name': 'usr', 'type': NodeType.directory}]

        self.assertEquals(Status.ok, r['status'])
        self.assertListEqual(items, r['items'])

    def test_make_directory(self):
        r = self.ns.make_directory('/my/dir/')
        self.assertEquals(Status.ok, r['status'])

        r = self.ns.make_directory('/my/dir/and/new/dir')
        self.assertEquals(Status.ok, r['status'])

        d = self.ns.get_file_info('/my/dir/and/new/dir')
        self.assertEquals(NodeType.directory, d['type'])

    def test_make_directory_with_error(self):
        self.ns.create_file({'path': '/my/dir/file', 'size': 0, 'chunks': {}})
        r = self.ns.make_directory('/my/dir/file/my_dir')
        self.assertEquals(Status.error, r['status'])
        r = self.ns.get_file_info('/my/dir/file/my_dir')
        self.assertEquals(Status.not_found, r['status'])

    def test_make_directory_already_exists(self):
        self.ns.make_directory('/my/dir/file')
        r = self.ns.make_directory('/my/dir/file')
        self.assertEquals(Status.already_exists, r['status'])

    def test_size_of(self):
        self.ns.create_file({'path': '/my/file', 'size': 100, 'chunks': {}})
        self.ns.create_file({'path': '/my/file2', 'size': 150, 'chunks': {}})

        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/subdir/file5', 'size': 250, 'chunks': {}})

        r = self.ns.size_of('/my')
        self.assertEquals(Status.ok, r['status'])
        self.assertEquals(100+150+200+250, r['size'])

    def test_size_of_not_found(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/subdir/file5', 'size': 250, 'chunks': {}})

        r = self.ns.size_of('/my/some/path')
        self.assertEquals(Status.not_found, r['status'])

    def test_create_file_exists(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        r = self.ns.create_file({'path': '/my/dir/file3', 'size': 250, 'chunks': {}})

        self.assertEquals(Status.already_exists, r['status'])

    def test_delete_file(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.delete('/my/dir/file3')

        r = self.ns.get_file_info('/my/dir/file3')
        self.assertEquals(Status.not_found, r['status'])

if __name__ == '__main__':
    unittest.main()
