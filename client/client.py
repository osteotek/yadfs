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
            os.environ["YAD_NS"] = "http://localhost:8888"
        self.ns = ServerProxy(os.environ["YAD_NS"])

    def list_dir(self, dir_path):
        return self.ns.list_directory(dir_path)

    def create_dir(self, path):
        return self.ns.make_directory(path)

    def delete_dir(self, path):
        return self.ns.delete_dir(path)

    def create_file(self, path):
        r = self._get_cs(path)
        cs_addr = r['cs']
        cs = ServerProxy(cs_addr)
        chunks = self.split_file(path)
        data={}
        data['path'] = path
        data['size'] = os.stat(path).st_size
        data['chunks'] = {}
        for count, chunk in enumerate(chunks):
            cs.upload_chunk(path + '_{0}'.format(str(count)), chunk)
            data['chunks'][path+'_'+str(count)] = cs_addr

        return self.ns.create_file(data)

    def delete_file(self, path):
        return self.ns.delete(path)

    def download_file(self, path):
        pass

    def path_status(self, path):
        return self.ns.get_file_info(path)

    def _get_cs(self, path):
        return self.ns.get_cs(path)

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
