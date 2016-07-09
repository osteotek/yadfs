import os
from xmlrpc.client import ServerProxy
from enum import Enum


class FileType(Enum):
    none = 0
    file = 1
    directory = 2

class Client:
    def __init__(self):
        self.chunk_servers = []
        if not os.getenv('YAD_NS'):
            os.environ["YAD_NS"] = "http://localhost:8888"
            self.ns = ServerProxy(os.environ["YAD_NS"])

    def start(self):
        print('send test request to ns')
        r = self.ns.list_directory('/')
        print(r)

    @staticmethod
    def splitfile(self, filename, chunksize=1024):
        if not os.path.isfile(filename):
            raise IOError('No such file as: {0}'.format(filename))

        filesize = os.stat(filename).st_size

        with open(filename, 'r') as fr:
            n_chunks = filesize // chunksize
            chunks = []
            print('splitfile: No of chunks required: {0}'.format(str(n_chunks + 1)))
            for i in range(n_chunks + 1):
                data = fr.read(chunksize)
                chunks.append(data)
                #with open(filename + "_{0}".format(str(i)), 'xb') as fw:
                #    fw.write(data)
            return chunks

    @staticmethod
    def combinefile(self, filename, chunks):
        if os.path.isfile(filename):
            raise IOError('Such file already exists: {0}'.format(filename))

        with open(filename, 'x') as fw:
            for chunk in chunks:
                fw.write(chunk)

    def list_dir(self, dir_path):
        ls = self.ns.list_directory(dir_path)
        return ls

    def create_dir(self, path):
        res = self.ns.make_directory(path)
        return res

    def delete_dir(self, path):
        res = self.ns.delete_dir(path)
        return res

    def create_file(self, path, content):
        res = self.ns.create_file(path, content)
        return res

    def delete_file(self, path):
        res = self.ns.delete_file(path)
        return res

    def path_status(self, path):
        return "neither"

    # for testing purposes
    def add_chunk_server(self, cs_addr):
        cs = ServerProxy(cs_addr)
        self.chunk_servers.append(cs)

    def write(self):
        for cs in self.chunk_servers:
            cs.upload_chunk("/folder/chunk1", "123")

    def read(self):
        for cs in self.chunk_servers:
            f = cs.get_chunk("/folder/chunk1")
            print(f)

    def delete(self):
        for cs in self.chunk_servers:
            cs.delete_chunk("/folder/chunk1")


# # first arg - NS address - http://localhost:888
# if __name__ == '__main__':
#     cl = Client(sys.argv[1])
#
    # cl.start()
    # cl.add_chunk_server("http://localhost:9999")
    # cl.write()
    # cl.read()
    # cl.delete()
