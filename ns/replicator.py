from queue import Queue
from xmlrpc.client import ServerProxy
import _thread


class Replicator:
    def __init__(self, ns):
        self.ns = ns
        self.queue = Queue()
        self.on = True

    def start(self):
        print('Start replication workers')
        _thread.start_new_thread(self._replicate_worker, ())

    def put_in_queue(self, path, existing_cs):
        item = (path, existing_cs)
        self.queue.put(item)

    def _replicate_worker(self):
        while self.on:
            item = self.queue.get()
            self.replicate(item)

    def replicate(self, item):
        if item is None:
            return

        path = item[0]
        cs_list = item[1]

        alive = [x for x in cs_list if self.ns._is_alive_cs(x)]

        if len(alive) == 0:
            print('All CS are down :(')
            return

        if len(alive) >= 2:
            print('File', path, 'is already replicated to more than 2 nodes')
            return

        new_cs = self.ns._select_available_cs(alive)

        if new_cs is None:
            print("Can't find available cs for replication", path)
        else:
            try:
                cl = ServerProxy(alive[0])
                cl.replicate_chunk(path, new_cs)
                cs_list.append(new_cs)
                print("File", path, "replicated to", new_cs)
            except:
                print('Error during replicatiion', path, 'to', new_cs)