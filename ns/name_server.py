#!/usr/bin/env python3
import os
import random
import sys
import yaml
import _thread
import socket
from datetime import datetime
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

from os.path import dirname

sys.path.append(dirname(dirname(__file__)))
from ns.replicator import Replicator
from utils.enums import NodeType, Status
from ns.file_node import FileNode


class NameServer:
    def __init__(self, dump_on=True):
        self.root = FileNode('/', NodeType.directory)
        self.dump_on = dump_on
        self.dump_path = "./name_server.yml"
        self.cs_timeout = 2  # chunk server timeout in seconds
        self.cs = {}  # chunk servers which should be detected by heartbeat
        self.repl = Replicator(self)

    # start name server
    # init heartbeat threads
    def start(self):
        self._load_dump()
        self.repl.start()

    def _load_dump(self):
        # try to read file from dump
        if os.path.isfile(self.dump_path):
            print("Try to read file tree from dump file", self.dump_path)
            with open(self.dump_path) as f:
                self.root = yaml.load(f)
            print("File tree has loaded from the dump file")
        else:
            print("There is no detected dump file")

    # dump file-tree to file if self.dump_on is True
    def _dump(self):
        if self.dump_on:
            try:
                with open(self.dump_path, 'w') as outfile:
                    outfile.write(yaml.dump(self.root))
            except Exception as e:
                print("Error dumping file", self.dump_path, e)

    # get name where to put CS file
    # path in format like /my_dir/usr/new_file
    # returns { 'status': Status.not_found} if there are no available cs
    # if ok return { 'status': Status.ok, 'cs': cs_address }
    def get_cs(self, path):
        if self.root.find_path(path) is not None:
            print(self.root.find_path(path))
            return {'status': Status.already_exists}

        cs = self._select_available_cs()
        if cs is None:
            print("available cs not found")
            return {'status': Status.not_found}

        return {'status': Status.ok, 'cs': cs}

    def _select_available_cs(self, ignore_cs=None):
        if ignore_cs is None:
            ignore_cs = []

        live = []
        for cs_name in self.cs:
            if self._is_alive_cs(cs_name) and cs_name not in ignore_cs:
                live.append(cs_name)

        if len(live) == 0:
            return None

        i = random.randint(0, len(live) - 1)
        return live[i]

    def _is_alive_cs(self, cs_addr):
        if cs_addr not in self.cs:
            return False

        last_hb = self.cs[cs_addr]
        now = datetime.now()
        diff = (now - last_hb).total_seconds()
        return diff <= self.cs_timeout

    # create file in NS after its chunks were created in CS
    # data.path = full path to the file
    # data.size = file size
    # data.chunks = {'chunk_name_1': cs-1, 'chunk_name_2': cs-2} /dictionary
    # returns { 'status': Status.ok } in case of success
    # Status.error - in case of error during file creation
    # Status.already_exists - file is already created
    def create_file(self, data):
        file = self.root.find_path(data['path'])
        print(file)
        if file is not None:
            return {'status': Status.already_exists}

        file = self.root.create_file(data['path'])
        if file == "Error":
            return {'status': Status.error}

        file.size = data['size']
        for k, v in data['chunks'].items():
            file.chunks[k] = [v]
            print('Chunk',k,'saved on', v)

        for c in file.chunks:
            self.repl.put_in_queue(c, file.chunks[c])  # put chunk to replication queue

        self._dump()
        print("Created file " + data['path'] + ' of size ' + str(data['size']))

        return {'status': Status.ok}

    # delete file\directory by specified path
    # r: {'status: Status.ok} if deleted
    # Status.error if you try to delete root
    # Status.not_found if path not found
    def delete(self, path):
        print("Delete", path)
        item = self.root.find_path(path)
        if item is None:
            return {'status': Status.not_found}

        if item.is_root:
            return {'status': Status.error}

        item.delete()
        self._dump()
        _thread.start_new_thread(self.delete_from_chunk_servers, (item,))
        return {'status': Status.ok}

    def delete_from_chunk_servers(self, file):
        if file.type == NodeType.directory:
            for c in list(file.children):
                self.delete_from_chunk_servers(c)
        else:
            print('Start delete file', file.get_full_path())
            for f_path, servers in file.chunks.items():
                for cs in servers:
                    try:
                        cl = ServerProxy(cs)
                        print('Send delete', f_path, 'to', cs)
                        cl.delete_chunk(f_path)
                    except:
                        print('Failed to delete', f_path, 'from', cs)

    # get file\directory info by given path
    # path format: /my_dir/index/some.file
    # response format:
    # { 'status': Status.ok
    #   'type': NodeType.type
    #   'path': '/my_dir/index/some.file' - full path for directory
    #   'size': 2014 - size in bytes
    #   'chunks': {
    #       '/my_dir/index/some.file_0': cs-2
    #   }
    def get_file_info(self, path):
        file = self.root.find_path(path)
        if file is None:
            return {'status': Status.not_found}

        chunks = {}
        for c_path, val in file.chunks.items():
            chunks[c_path] = val[0]

        return {'status': Status.ok,
                'type':   file.type,
                'path':   file.get_full_path(),
                'size':   file.size,
                'date':   file.date,
                'chunks': chunks}

    # creates directory by the given path
    # response: { 'status': Status.ok }
    # Status.error - if error and directory not created
    # States.already_exists - if directory is already exists by given path
    def make_directory(self, path):
        d = self.root.find_path(path)

        if d is not None:
            return {'status': Status.already_exists}

        d = self.root.create_dir(path)
        if d == "Error":
            return {'status': Status.error}

        return {'status': Status.ok}

    # execute ls command in directory
    # response:
    # { 'status': Status.ok,
    #   'items': { - dict of items
    #       item_name: dict from get_file_info()
    #       item_name2: dict from get_file_info() }
    # }
    # if file not found: {'status': Status.not_found}
    def list_directory(self, path):
        print('request to list directory ' + path)
        directory = self.root.find_path(path)
        if directory is None:
            return {'status': Status.not_found}

        items = {}
        for f in directory.children:
            # items[f.name] = f.type
            items[f.name] = self.get_file_info(f.get_full_path())

        result = {'status': Status.ok, 'items': items}
        return result

    # return size of the file\directory by the given path
    # size of directory returns size of its children
    # r: { 'status': Status.ok\Status.not_found, 'size': size by path}    #
    def size_of(self, path):
        i = self.root.find_path(path)
        if i is None:
            return {'status': Status.not_found, 'size': 0}

        return {'status': Status.ok, 'size': i.size}

    # get heartbeat from chunk server
    def heartbeat(self, cs_addr):
        if cs_addr not in self.cs:
            print('register CS ' + cs_addr)

        self.cs[cs_addr] = datetime.now()
        return {'status': Status.ok}

# args: host and port: localhost 8888
if __name__ == '__main__':
    #if len(sys.argv) == 3:
    #    host = sys.argv[1]
    #    port = int(sys.argv[2])
    #else:
    #    host = 'localhost'
    #    port = 8888

    host = socket.gethostbyname(socket.gethostname())
    port = 8888
    ns = NameServer(dump_on=True)
    ns.start()

    server = SimpleXMLRPCServer((host, port), logRequests=False)
    server.register_introspection_functions()
    server.register_instance(ns)
    server.serve_forever()
