import sys
from xmlrpc.client import ServerProxy


class Client:
    def __init__(self, addr):
        self.ns = ServerProxy(addr)

    def start(self):
        print("send test request to ns")
        r = self.ns.list_directory("some_path")
        print(r)

# first arg - NS address - http://localhost:888
if __name__ == '__main__':
    cl = Client(sys.argv[1])
    cl.start()