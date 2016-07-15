import os
import errno
from xmlrpc.client import ServerProxy

import sys
from os.path import dirname
sys.path.append(dirname(dirname(__file__)))
from utils.enums import NodeType, Status


class Client:
    def __init__(self):
        self.chunk_servers = []
        if not os.getenv('YAD_NS'):
            os.environ['YAD_NS'] = 'http://localhost:8888'
        self.ns = ServerProxy(os.environ['YAD_NS'])

    def list_dir(self, dir_path):
        return self.ns.list_directory(dir_path)

    def create_dir(self, path):
        return self.ns.make_directory(path)

    def delete_dir(self, path):
        return self.ns.delete_dir(path)

    def create_file(self, path, remote_path):
        fn = path.split("/")[-1]
        remote_filepath = os.path.join(remote_path, fn)
        r = self._get_cs(remote_filepath)
        if not r['status'] == Status.ok:
            return r
        cs_addr = r['cs']
        cs = ServerProxy(cs_addr)
        chunks = self.split_file(path)
        data={}
        data['path'] = remote_filepath
        data['size'] = os.stat(path).st_size
        data['chunks'] = {}
        for count, chunk in enumerate(chunks):
            cs.upload_chunk(remote_filepath + '_{0}'.format(str(count)), chunk)
            data['chunks'][remote_filepath+'_'+str(count)] = cs_addr

        return self.ns.create_file(data)

    def delete(self, path):
        return self.ns.delete(path)

    def download_file(self, path, dst_path):
        result, content = self.get_file_content(path)
        if result != Status.ok:
            return {'status': result}

        fn = path.split("/")[-1]
        self.make_sure_path_exists(dst_path)
        file_path = os.path.join(dst_path, fn)
        with open(file_path, "w") as f:
            f.write(content)
            return {'status': Status.ok}

    def get_file_content(self, path):
        info = self.ns.get_file_info(path)
        if info['status'] != Status.ok:
            return info['status'], None

        chunks = info['chunks']
        content = ""
        data = {}
        for chunk, addr in chunks.items():
            cs = ServerProxy(addr)
            chunk_data = cs.get_chunk(chunk)
            index = int(chunk.split("_")[-1])
            data[index] = chunk_data

        i = 0
        while i < len(data):
            content += data[i]
            i += 1

        return Status.ok, content

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

    @staticmethod
    def make_sure_path_exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
