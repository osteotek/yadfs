from utils.enums import NodeType, Status
import time


class FileNode:
    def __init__(self, name, node_type):
        self.name = name
        self.parent = None
        self.children = []
        self.type = node_type
        self._size = 0  # size in bytes
        self.date = time.time()

        # dictionary of file chunks, empty for the directory
        self.chunks = {}

    @property
    def is_root(self):
        return self.parent is None

    @property
    def size(self):
        if self.type == NodeType.directory:
            return sum(c.size for c in self.children)

        return self._size

    @size.setter
    def size(self, value):
        self._size = value

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    # find file node by path like (/dir/ReadMe.txt)
    # first '/' is a path from the current node
    def find_path(self, path):
        if path == self.name:
            return self

        ch_name, ch_path = self._extract_child_path_and_name(path)

        for child in self.children:
            if child.name == ch_name:
                return child.find_path(ch_path)

        # return None if file\directory was not found
        return None

    @staticmethod
    def _extract_child_path_and_name(path):
        path = path.strip('/')
        next_sep = path.find('/')

        # if path does not contain sub-folders
        # find child by the path
        if next_sep == -1:
            ch_name = path
            ch_path = path
        else:
            # if path contains sub-folders than extract child name from the path and recursively look
            # in child sub-folders
            ch_name = path[0:next_sep]
            ch_path = path[next_sep + 1:]
        return ch_name, ch_path

    @staticmethod
    # extract from path like /usr/bin/file.txt - file.txt and /usr/bin
    # os.path.split(path) does exactly that
    def _extract_file_name_and_file_dir(path):
        path = path.strip('/')
        sep = path.rfind('/')

        # if path does not contain sub-folders
        # find child by the path
        if sep == -1:
            f_name = path
            f_dir = None
        else:
            # if path contains sub-folders than extract child name from the path and recursively look
            # in child sub-folders
            f_name = path[sep + 1: len(path)]
            f_dir = path[: sep]
        return f_name, f_dir

    # recursively creates directory or directories from path
    # it returns error if already exists file with the same name as directory
    def create_dir(self, path):
        dirs = path.strip('/').split('/')
        curr_dir = self
        i = 0
        while i < len(dirs):
            d_name = dirs[i]

            directory = next((x for x in curr_dir.children if x.name == d_name), None)
            if directory is None:
                directory = FileNode(d_name, NodeType.directory)
                curr_dir.add_child(directory)

            elif directory.type == NodeType.file:
                print("Can't create directory because of wrong file name " + d_name)
                return "Error"
            curr_dir = directory
            i += 1

        return curr_dir

    def create_file(self, path):
        f_name, f_dir = self._extract_file_name_and_file_dir(path)

        if f_dir is not None:
            directory = self.create_dir(f_dir)

            if directory == "Error":
                return 'Error'
        else:
            directory = self

        file = FileNode(f_name, NodeType.file)
        directory.add_child(file)
        return file

    def find_file_by_chunk(self, path):

        i = path.rfind('_')
        if i is None:
            print("Incorrect chunk path", path)
            return Status.error

        file_name = path[:i]
        return self.find_path(file_name)

    # delete node from the file tree
    # you can't delete root!
    def delete(self):
        if not self.is_root:
            self.parent.children.remove(self)
            self.parent = None

    # returns full path of the node
    # like /var/img/my_file.txt
    def get_full_path(self):
        if self.is_root:
            return ''
        else:
            return self.parent.get_full_path() + '/' + self.name

    def get_full_dir_path(self):
        if self.is_root:
            return ''

        path = self.parent.get_full_dir_path()
        if self.type == NodeType.directory:
            path += '/' + self.name

        return path
