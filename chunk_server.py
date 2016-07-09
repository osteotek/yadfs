from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from enum import Enum
import sys
import uuid
import os
import errno


class Result(Enum):
    ok = 1
    not_ok = 2


class ChunkServer:
    def __init__(self, ns_addr):
        self.ns = ServerProxy(ns_addr)
        self.uuid = uuid.uuid1()
        self.local_fs_root = "/tmp/yadfs/chunks"
        if not os.access(self.local_fs_root, os.W_OK):
            os.makedirs(self.local_fs_root)

    def heartbeat(self):
        self.ns.heartbeat(self.uuid)

    def upload_chunk(self, chunk_path, chunk):
        local_path = self.chunk_filename(chunk_path)
        print(local_path)
        ldir = os.path.dirname(local_path)
        self.make_sure_path_exists(ldir)
        with open(local_path, "w") as f:
            f.write(chunk)
            return "ok"

    def get_chunk(self, chunk_path):
        local_path = self.chunk_filename(chunk_path)
        with open(local_path, "r") as f:
            data = f.read()
        return data

    def delete_chunk(self, chunk_path):
        local_path = self.chunk_filename(chunk_path)
        ldir = os.path.dirname(local_path)
        self.make_sure_path_exists(ldir)
        if os.path.exists(local_path):
            os.remove(local_path)
            return "ok"
        else:
            return "not_ok"

    def replicate_chunk(self, chunk_path, cs_addr):
        cs = ServerProxy(cs_addr)
        chunk = self.get_chunk(chunk_path)
        cs.upload_chunk(chunk_path, chunk)

    def chunk_filename(self, chunk_path):
        return os.path.join(self.local_fs_root, chunk_path)

    @staticmethod
    def make_sure_path_exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

# ars: host and port: localhost 9999
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("You have to specify host and port!")

    host = sys.argv[1]
    port = int(sys.argv[2])

    server = SimpleXMLRPCServer((host, port))
    server.register_introspection_functions()
    server.register_instance(ChunkServer("http://localhost:8888"))
    server.serve_forever()
