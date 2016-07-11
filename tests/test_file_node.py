import unittest

from ns.file_node import FileNode
from utils.enums import NodeType


class FileNodeTests(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.root = FileNode("root", NodeType.directory)

    def test_find_node(self):
        file = FileNode("test.txt", NodeType.file)
        self.root.add_child(file)
        self.assertEqual(file, self.root.find_path("/test.txt"))

    def test_among_several_files(self):
        file = FileNode("test1.txt", NodeType.file)
        self.root.add_child(file)

        file = FileNode("test2.txt", NodeType.file)
        self.root.add_child(file)

        file = FileNode("another_file", NodeType.file)
        self.root.add_child(file)

        self.assertEqual(file, self.root.find_path("/another_file"))

    def test_find_in_sub_folders(self):
        parent = self.root
        folder = FileNode("usr", NodeType.directory)
        parent.add_child(folder)

        parent = folder
        folder = FileNode("bin", NodeType.directory)
        parent.add_child(folder)

        parent = folder
        folder = FileNode("local", NodeType.directory)
        parent.add_child(folder)

        self.assertEqual(folder, self.root.find_path("/usr/bin/local"))

    def test_if_not_found_return_none(self):
        parent = self.root
        folder = FileNode("usr", NodeType.directory)
        parent.add_child(folder)

        parent = folder
        folder = FileNode("bin", NodeType.directory)
        parent.add_child(folder)

        parent = folder
        folder = FileNode("local", NodeType.directory)
        parent.add_child(folder)

        self.assertEqual(None, self.root.find_path("/usr/ans"))

    def test_create_directory(self):
        r = self.root.create_dir("/etc/nginx/log")
        self.assertNotEqual("Error", r)

        dir = self.root.find_path("/etc/nginx/log")
        self.assertEqual("log", dir.name)
        self.assertEqual("nginx", dir.parent.name)
        self.assertEqual("etc", dir.parent.parent.name)

    def test_create_file_dir(self):
        self.root.create_dir("/etc/nginx/")
        f1 = self.root.create_file("/etc/nginx/file_dir1/file_dir2/my_file")

        f2 = self.root.find_path("/etc/nginx/file_dir1/file_dir2/my_file")
        self.assertEqual(f1, f2)

    def test_full_path(self):
        f = self.root.create_file("/etc/nginx/file_dir1/file_dir2/my_file")
        self.assertEqual("/etc/nginx/file_dir1/file_dir2/my_file", f.get_full_path())

    def test_full_directory_path_from_file(self):
        f = self.root.create_file("/etc/nginx/another/f1/f2/my_file")
        self.assertEqual("/etc/nginx/another/f1/f2", f.get_full_dir_path())

    def test_full_directory_path_from_dir(self):
        f = self.root.create_dir("/etc/nginx/another/f1/f2/f3")
        self.assertEqual("/etc/nginx/another/f1/f2/f3", f.get_full_dir_path())

if __name__ == '__main__':
    unittest.main()
