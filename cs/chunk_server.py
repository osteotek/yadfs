#!/usr/bin/env python3
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Error
import sys
import os
import errno
import time
import _thread
from os.path import dirname
sys.path.append(dirname(dirname(__file__)))
from utils.enums import NodeType, Status


class ChunkServer:
    def __init__(self, addr, ns_addr):
        self.ns = ServerProxy(ns_addr)
        self.addr = addr
        self.ns_addr = ns_addr
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
                self.ns.heartbeat('http://' + self.addr)
            except Exception as e:
                pass
            time.sleep(self.hb_timeout)

    def upload_chunk(self, chunk_path, chunk):
        print('Upload file', chunk_path)
        local_path = self.chunk_filename(chunk_path)
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
            print('Delete file', chunk_path)
            return {'status': Status.ok}
        else:
            return {'status': Status.not_found}

    def replicate_chunk(self, chunk_path, cs_addr):
        try:
            cs = ServerProxy(cs_addr)
            print("Replicate", chunk_path, 'to', cs_addr)
            chunk = self.get_chunk(chunk_path)
            cs.upload_chunk(chunk_path, chunk)
            print("File", chunk_path, "has replicated to", cs_addr)
            return "ok"
        except:
            print("Replication of", chunk_path, 'failed')

    def chunk_filename(self, chunk_path):
        if chunk_path[0] == '/':
            return os.path.join(self.local_fs_root, chunk_path[1:])
        else:
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
    if len(sys.argv) < 2:
        print("You have to specify address of chunk server")
        exit()

    host = sys.argv[1]
    if sys.argv[2]:
        port = sys.argv[2]
    else:
        port = 9999


    addr = host + ":" + str(port)
    if not os.getenv('YAD_NS'):
        ns_addr = 'http://localhost:8888'
    else:
        ns_addr = os.environ['YAD_NS']
    cs = ChunkServer(addr, ns_addr)
    cs.start()

    server = SimpleXMLRPCServer((host, port))
    server.register_introspection_functions()
    server.register_instance(cs)
    server.serve_forever()
