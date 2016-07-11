import os
from xmlrpc.client import ServerProxy

import sys
from os.path import dirname
sys.path.append(dirname(dirname(__file__)))
from utils.enums import NodeType, Status


class Client:
    def __init__(self):
        self.chunk_servers = []
        if not os.getenv('YAD_NS'):
            os.environ["YAD_NS"] = "http://localhost:8889"
        self.ns = ServerProxy(os.environ["YAD_NS"])

    def start(self):
        print('send test request to ns')
        r = self.ns.list_directory('/')
        print(r)

    def list_dir(self, dir_path):
        ls = self.ns.list_directory(dir_path)
        return ls['items']

    def create_dir(self, path):
        res = self.ns.make_directory(path)
        return res

    def delete_dir(self, path):
        res = self.ns.delete_dir(path)
        return res

    def create_file(self, path):
        cs = self._get_cs(path)
        cs_addr = cs['addr']
        cs_name = cs['name']
        cs = ServerProxy(cs_addr)
        chunks = self.split_file(path)
        data={}
        data['path'] = path
        data['size'] = os.stat(path).st_size
        data['chunks'] = {}
        for count, chunk in enumerate(chunks):
            cs.upload_chunk(path + '_{0}'.format(str(count)), chunk)
            data['chunks'][path+'_'+str(count)] = cs_name

        res = self.ns.create_file(data)
        return res

    def delete_file(self, path):
        res = self.ns.delete(path)
        return res

    def download_file(self, path):
        pass

    def path_status(self, path):
        return "neither"

    def _get_cs(self, path):
        res = self.ns.get_cs(path)
        return res

    @staticmethod
    def split_file(filename, chunksize=1024):
        if not os.path.isfile(filename):
            return Status.not_found

        with open(filename, 'r') as fr:
            data = fr.read()
        chunks = []
        while len(data) >= chunksize:
            i = chunksize
            while not data[i].isspace():
                if i == 0:
                    i = chunksize
                    break
                else:
                    i -= 1
            chunks.append(data[:i])
            data = data[i:]
        chunks.append(data)
        return chunks

    @staticmethod
    def write_combined_file(filename, chunks):
        if os.path.isfile(filename):
            return Status.already_exists

        with open(filename, 'x') as fw:
            for chunk in chunks:
                fw.write(chunk)
