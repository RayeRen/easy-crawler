import unittest
from queue import Queue

from core.crawler import Crawler


class MockCrawler(Crawler):

    @staticmethod
    def prepare(context, args):
        pass

    @property
    def base_url(self):
        return "www.mock.com"

    @staticmethod
    def collect_results(context, result):
        pass

    @property
    def task_name(self):
        return "mock"

    def parse(self, soup, url):
        pass


class TestCrawler(unittest.TestCase):
    def setUp(self):
        q_results = Queue()
        q_stats = Queue()
        self.c = MockCrawler("www.mock.com", q_results, q_stats, 0, 1)

    def test_redis(self):
        pass


if __name__ == '__main__':
    unittest.main()
