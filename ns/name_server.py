import sys
from xmlrpc.server import SimpleXMLRPCServer
from ns.file_node import FileNode
from enums import NodeType, Status

class NameServer:
    def __init__(self):
        self.root = FileNode('root', NodeType.directory)

    # get name where to put CS file
    # path in format like /my_dir/usr/new_file
    def get_cs(self, path):
        if self.root.find_path(path) is not None:
            return 'file already exists'
        return "cs-1"

    # create file in NS after its chunks were created in CS
    # data.path = full path to the file
    # data.size = file size
    # data.chunks = {'chunk_name_1': cs-1, 'chunk_name_2': cs-2} /dictionary
    # returns { 'status': Status.ok } in case of success
    def create_file(self, data):
        file = self.root.create_file(data['path'])
        if file == "Error":
            return {'status': Status.error}

        file.size = data['size']
        for k, v in data['chunks'].items():
            file.chunks[k] = [v]

        print("Created file " + data['path'])
        return {'status': Status.ok}

    # delete file by specified path
    def delete_file(self, path):
        return {'status': Status.ok}

    # get file\directory info by given path
    # path format: /my_dir/index/some.file
    # response format:
    # { 'status': Status.ok
    #   'type': NodeType.type
    #   'path': '/my_dir/index/some.file' - full path for directory
    #   'size': 2014 - size in bytes
    #   'chunks': { cs - name of chunk server, path - path to the chunk
    #       'some.file_0': { 'cs': 'cs-2', 'path': '/my_dir/index/some.file_0'},
    #       'some.file_1': { 'cs': 'cs-1', 'path': '/my_dir/index/some.file_1'}
    #   }
    def get_file_info(self, path):
        file = self.root.find_path(path)
        if file is None:
            return {'status': Status.not_found}

        chunks = {}
        for c_name, val in file.chunks.items():
            chunks[c_name] = {'cs': val[0], 'path': file.get_full_dir_path() + '/' + c_name}

        return {'status': Status.ok,
                'type': file.type,
                'path': file.get_full_path(),
                'size': file.size,
                'chunks': chunks}

    def make_directory(self, path):
        directory = self.root.create_dir(path)
        if directory == "Error":
            return {'status': Status.error}

        return {'status': Status.ok}

    # execute ls command in directory
    # response:
    # { 'status': Status.ok,
    #   'items': [ - list of items
    #       {'name': item_name, 'type': NodeType},
    #       {'name': item_name2, 'type': NodeType}]
    # }
    # if file not found: {'status': Status.not_found}
    def list_directory(self, path):
        directory = self.root.find_path(path)
        if directory is None:
            return {'status': Status.not_found}

        items = ({'name': f.name, 'type': f.type} for f in directory.children)
        return {'status': Status.ok, 'items': list(items)}

# ars: host and port: localhost 888
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("You have to specify host and port!")

    host = sys.argv[1]
    port = int(sys.argv[2])

    server = SimpleXMLRPCServer((host, port))
    server.register_introspection_functions()
    server.register_instance(NameServer())
    server.serve_forever()
