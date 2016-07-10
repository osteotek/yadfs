import unittest
import os

from client import Client


class ClientTests(unittest.TestCase):

    def setUp(self):
        with open('test_small.txt', 'x') as test_small:
            test_small.write(self.content[:12])
        with open('test_large.txt', 'x') as test_large:
            test_large.write(self.content)

    def tearDown(self):
        os.remove('test_small.txt')
        os.remove('test_large.txt')

    def test_split_small_file(self):
        chunks = Client.split_file('test_small.txt')
        self.assertEqual(chunks[0], self.content[:12])
        self.assertEqual(len(chunks), 1)

    def test_split_large_file(self):
        chunks = Client.split_file('test_large.txt')
        self.assertEqual(chunks[1], self.content[1024:])
        self.assertEqual(chunks[0]+chunks[1], self.content)

    content = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\nSed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?'

if __name__ == '__main__':
    unittest.main()
