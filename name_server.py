from xmlrpc.server import SimpleXMLRPCServer
import sys
from enum import Enum


class NodeType(Enum):
    file = 1
    directory = 2


class FileNode:
    def __init__(self, name, node_type):
        self.name = name
        self.parent = None
        self.children = []
        self.type = node_type

        # list of file chunks, empty for the directory
        self.chunks = {}

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

    # recursively creates directory or directories from path
    # it returns error if already exists file with the same name as directory
    def create_dir(self, path):
        ch_name, ch_path = self._extract_child_path_and_name(path)

        # check if directory if already created
        dir = next((x for x in self.children if x.name == ch_name), [None])[0]
        if dir is None:
            dir = FileNode(ch_name, NodeType.directory)
            self.add_child(dir)

        elif dir.type == NodeType.file:
            return "Error", "Can't create directory because of wrong file name"

        # if path does not have any sub-folders
        if ch_name == ch_path:
            return "Ok"

        # create directories for sub-folders
        return dir.create_dir(ch_path)

    def create_file(self, path):
        pass

class NameServer:
    def __init__(self):
        self.root = FileNode('root', NodeType.directory)

    def get_cs(self):
        return "cs-1"

    def create_file(self, data):
        file = FileNode(data['name'], NodeType.file)
        return "ok"

    def delete_file(self, path):
        return "ok"

    def locate_file(self, path):
        return "ok"

    def make_directory(self, path):
        return self.root.create_dir(path)

    def list_directory(self, path):
        return "ok"


# ars: host and port: localhost 888
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("You have to specify host and port!")

    host = sys.argv[1]
    port = int(sys.argv[2])

    server = SimpleXMLRPCServer((host, port))
    server.register_introspection_functions()
    server.register_instance(NameServer())
    server.serve_forever()
