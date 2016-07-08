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

    def find_path(self):
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
        return "ok"

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
