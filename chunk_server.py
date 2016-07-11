from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from enums import NodeType, Status
import sys
import os
import errno
import time
import _thread


class ChunkServer:
    def __init__(self, name, addr, ns_addr):
        self.ns = ServerProxy(ns_addr)
        self.addr = addr
        self.name = name
        self.local_fs_root = "/tmp/yadfs/chunks"
        self.hb_timeout = 0.5  # heartbeat timeout in seconds
        self.on = True

    def start(self):
        print('Init server')
        if not os.access(self.local_fs_root, os.W_OK):
            print('Create directory for storage:', self.local_fs_root,)
            os.makedirs(self.local_fs_root)

        print('Start sending heartbeats')
        _thread.start_new_thread(self._heartbeat, ())
        print('Server is ready')

    def _heartbeat(self):
        while self.on:
            try:
                self.ns.heartbeat(self.name, 'http://' + self.addr)
            except:
                pass  # ignore error during hb - send it in the next time
            time.sleep(self.hb_timeout)

    def upload_chunk(self, chunk_path, chunk):
        local_path = self.chunk_filename(chunk_path)
        print(local_path)
        ldir = os.path.dirname(local_path)
        self.make_sure_path_exists(ldir)
        with open(local_path, "w") as f:
            f.write(chunk)
            return {'status': Status.ok}

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
            return {'status': Status.ok}
        else:
            return {'status': Status.not_found}

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
    if len(sys.argv) < 4:
        print("You have to specify host, port and name")
        exit()

    host = sys.argv[1]
    port = int(sys.argv[2])
    name = sys.argv[3]

    addr = host + ":" + str(port)
    cs = ChunkServer(name,  addr, "http://localhost:8889")
    cs.start()

    server = SimpleXMLRPCServer((host, port))
    server.register_introspection_functions()
    server.register_instance(cs)
    server.serve_forever()
