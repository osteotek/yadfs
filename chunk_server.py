from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from enum import Enum
import sys
import uuid
import os


class Result(Enum):
    ok = 1
    not_ok = 2


class ChunkServer:
    def __init__(self, ns_addr):
        self.ns = ServerProxy(ns_addr)
        self.uuid = uuid.uuid1()
        self.local_fs_root = "/tmp/yadfs/chunks/"
        if not os.access(self.local_fs_root, os.W_OK):
            os.makedirs(self.local_fs_root)

    def heartbeat(self):
        self.ns.heartbeat(self.uuid)

    def upload_chunk(self, chunk_path, chunk):
        local_path = self.chunk_filename(chunk_path)
        with open(local_path, "w") as f:
            f.write(chunk)

    def get_chunk(self, chunk_path):
        local_path = self.chunk_filename(chunk_path)
        with open(local_path, "r") as f:
            data = f.read()
        return data

    def delete_chunk(self, chunk_path):
        local_path = self.chunk_filename(chunk_path)
        if os.path.exists(local_path):
            os.remove(local_path)
            return Result.ok
        else:
            return Result.not_ok

    def chunk_filename(self, chunk_path):
        return self.local_fs_root + "/" + chunk_path


# ars: host and port: localhost 999
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("You have to specify host and port!")

    host = sys.argv[1]
    port = int(sys.argv[2])

    server = SimpleXMLRPCServer((host, port))
    server.register_introspection_functions()
    server.register_instance(ChunkServer())
    server.serve_forever()
