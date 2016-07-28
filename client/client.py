import os
import errno
from xmlrpc.client import ServerProxy

import sys
from os.path import dirname
sys.path.append(dirname(dirname(__file__)))
from utils.enums import NodeType, Status


class Client:
    def __init__(self, ns_addr=None):
        self.chunk_servers = []
        if ns_addr is not None:
            os.environ['YAD_NS'] = ns_addr
        elif not os.getenv('YAD_NS'):
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

        if not os.path.isfile(path):
            return Status.not_found

        with open(path, 'r') as fr:
            data = fr.read()

        return self._save_file_to_dfs(data, remote_filepath)

    def _save_file_to_dfs(self, content, remote_filepath):
        r = self._get_cs(remote_filepath)
        if not r['status'] == Status.ok:
            return r
        cs_addr = r['cs']
        cs = ServerProxy(cs_addr)

        chunks = self.split_file(content)
        data = {}
        data['path'] = remote_filepath
        data['size'] = len(content)
        data['chunks'] = {}
        for count, chunk in enumerate(chunks):
            cs.upload_chunk(remote_filepath + '_{0}'.format(str(count)), chunk)
            data['chunks'][remote_filepath + '_' + str(count)] = cs_addr

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

    def get_file_info(self, path):
        info = self.ns.get_file_info(path)
        if info['status'] != Status.ok:
            return info['status'], None
        
        return Status.ok, info

    def get_chunk(self, path, chunk_id):
        info = self.ns.get_file_info(path)
        if info['status'] != Status.ok:
            return info['status'], None
        
        chunks = info['chunks']
        for chunk, addr in chunks.items():
            if int(chunk.split("_")[-1]) == chunk_id:
                cs = ServerProxy(addr)
                chunk_data = cs.get_chunk(chunk)
                return Status.ok, chunk_data

    def path_status(self, path):
        return self.ns.get_file_info(path)

    def _get_cs(self, path):
        return self.ns.get_cs(path)

    def get_chunk(self, path):
        r_index = path.rindex('_')
        f_path = path[:r_index]

        info = self.ns.get_file_info(f_path)
        for chunk, addr in info['chunks'].items():
            if chunk == path:
                cs = ServerProxy(addr)
                return {'status': Status.ok, 'data': cs.get_chunk(chunk)}

        return {'status': Status.not_found}

    def download_to(self, v_path, l_path):
        st, data = self.get_file_content(v_path)
        os.makedirs(os.path.dirname(l_path), exist_ok=True)
        with open(l_path, "w") as f:
            f.write(data)

        return {'status': Status.ok}

    def save(self, data, path):
        st = self._save_file_to_dfs(data, path)
        return {'status': st}

    @staticmethod
    def split_file(data, chunksize=1024):
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
