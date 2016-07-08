from xmlrpc.server import SimpleXMLRPCServer
import sys


class NameServer:
    def get_cs(self):
        return "cs-1"

    def create_file(self, data):
        return "ok"

    def delete_file(self, path):
        return "ok"

    def locate_file(self, path):
        return "ok"

    def make_directory(self, path):
        return "ok"

    def list_directory(self, path):
        return "ok"


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
