import random
import threading
import time
from queue import Empty

import OpenSSL
import redis
import requests
from bs4 import BeautifulSoup
from requests.exceptions import ProxyError, ConnectTimeout, SSLError, ReadTimeout

from .config import Config
from .crawler_scheduler import CrawlerScheduler
from .proxy_pool import PROXY_POOL_REGISTRY
from .utils import start_thread
import proxy_pools

requests.packages.urllib3.disable_warnings()


class Crawler:
    """
    Abstract Crawler
    """

    def __init__(self, task_name, proxy_pool, start_urls,
                 q_results, q_stats, q_log,
                 rank, thread_num, restart,
                 shared_context, args=None):
        self.rank = rank
        self.task_name = task_name
        self.args = args or {}
        self.restart = restart
        self.shared_context = shared_context
        self.log("Rank: %d started, with max %d threads." % (rank, thread_num))

        # redis
        rdp = redis.ConnectionPool(host=Config.REDIS_HOST,
                                   port=Config.REDIS_PORT, db=0,
                                   max_connections=1000)
        self.redis = redis.StrictRedis(connection_pool=rdp)

        self.start_urls = start_urls
        self.todo_key = self.task_name + "_todo"
        self.doing_key = self.task_name + "_doing"
        self.done_key = self.task_name + "_done"

        # multiprocess and multithreads
        self.max_thread_num = int(thread_num)
        self.current_thread_num = 0
        self.threads_status = [-1] * self.max_thread_num  # -1: available
        self.thread_locks = [threading.Lock() for _ in range(self.max_thread_num)]
        self.crawled = set()
        self.q_results = q_results

        # requests
        self.user_agents = [
            line.strip() for line in open('resources/agents_list.txt', encoding='utf-8').readlines() if
            line.strip() != ""
        ]
        self.s = requests.Session()
        self.proxy_pool = PROXY_POOL_REGISTRY[proxy_pool](self.redis, args)
        self.proxy_pool.collect_proxies()
        self.proxy_pool.shuffle_proxies()

        # stats and logs
        self.q_stats = q_stats
        self.q_log = q_log

    @property
    def base_url(self):
        raise NotImplementedError

    @staticmethod
    def prepare(context, args):
        """
        Do something preparation and return a list of start urls.
        Running in MAIN process, before all workers starting.
        :param context: store some variable shared in `collect_results`
        :param args: same as args passed to `start`
        :return: list of start urls
        """
        raise NotImplementedError

    def parse(self, soup, url):
        """
        You should do 2 steps here:
        1. Parse the html, extract useful information and save them by calling `add_result`
        2. Extract NEXT urls and save them by calling `add_url`, making the crawler continue running.

        You should NOT write any thread-UNSAFE code here, such as writing to a file. Instead, you should
        pass the result to `collect_results` by calling `add_result`.
        :param soup: html parsed by bs4
        :param url: cleaned request url
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def collect_results(context, result):
        """
        Handle the result. For example, save them to the file.
        You don't need to consider synchronization because it is running in ONE process.
        :param context: some variables saved in `collect_results`
        :param result: result added by `add_result` method
        """
        raise NotImplementedError

    @classmethod
    def start(cls, task_name, proxy_pool, thread_num, qps=None, restart=False, **kwargs):
        kwargs.update({
            'task_name': task_name,
            'proxy_pool': proxy_pool,
            'thread_num': thread_num,
            'qps': qps,
            'restart': restart
        })
        CrawlerScheduler(cls, **kwargs).run()

    def clean_url(self, url):
        """
        Clean the url here if you need. Url is the hash key deciding where one website was visited before.
        :param url:
        :return:
        """
        url = url.replace(self.base_url, "")
        url = url.strip()
        url = url.rstrip('/')
        url = url.replace('//', "/")
        if len(url) == 0 or url[0] != "/":
            url = url + "/"
        return url

    def run(self):
        if self.restart:
            if self.is_master():
                self.reset_task()
            else:
                time.sleep(3)

        if self.is_master():
            todo_urls = list(self.redis.smembers(self.doing_key))
            self.log("Collect %d proxies." % self.proxy_pool.proxies.qsize())
            self.log("%s task starts." % self.task_name)
            self.log("%d jobs in todo list. Rollback now." % (len(todo_urls)))
            self.log("%d jobs were completed already." % (self.redis.scard(self.done_key)))
            self.log("%d jobs were in the todo queue before rollback." % (self.redis.llen(self.todo_key)))
            todo_urls = self.redis.smembers(self.doing_key)
            for u in todo_urls:
                self.add_job(u.decode('utf-8'))
            self.log("%d jobs were in the todo queue after rollback." % (self.redis.llen(self.todo_key)))
            self.redis.delete(self.doing_key)

            # add starting urls
            for url in self.start_urls:
                self.add_job(url)
        else:
            time.sleep(3)

        while True:
            cur_max_threads_num = self.shared_context['cur_max_threads_num']
            if self.shared_context['terminate']:
                return

            for tid in range(self.max_thread_num):
                result = None
                with self.thread_locks[tid]:
                    if isinstance(self.threads_status[tid], tuple):
                        result = self.threads_status[tid]
                        self.threads_status[tid] = -1
                    available = self.threads_status[tid] == -1

                if available and tid < cur_max_threads_num:
                    if result is not None:
                        self.scrap_done(*result)
                    self.threads_status[tid] = 0
                    while True:
                        url_and_retry = self.pop_job()
                        if url_and_retry is not None:
                            start_thread(self.scrape, (tid, url_and_retry))
                            break
                        else:
                            working = False
                            for tid in range(self.max_thread_num):
                                if self.threads_status[tid] == 0:
                                    working = True
                                    break
                            if not working:
                                return

    def scrape(self, tid, url_and_retry):
        res = None
        retry = 10
        while retry > 0:
            proxy = self.proxy_pool.get_proxy()
            if proxy is not None:
                proxies = {'https': proxy, 'http': proxy}
            else:
                proxies = None
            try:
                headers = {'User-Agent': random.choice(self.user_agents)}
                res = requests.get(
                    self.base_url + url_and_retry[0],
                    proxies=proxies, timeout=5, verify=False,
                    headers=headers
                )
                if res.status_code == 200:
                    self.proxy_pool.feedback_proxy(proxy, level=0)
                    break
                else:
                    self.proxy_pool.feedback_proxy(proxy, level=1)
                    self.q_log.put('Status_code Error: url={}, code={}'.format(url_and_retry[0], res.status_code))
                    res = None
                    retry -= 1
            except ProxyError:
                self.proxy_pool.feedback_proxy(proxy, level=2)
                self.q_log.put('Proxy Error: url={}'.format(url_and_retry[0]))
                retry -= 1
            except (requests.exceptions.ConnectionError, ReadTimeout, ConnectTimeout, SSLError, OpenSSL.SSL.Error) as e:
                self.q_log.put('Connection Error: url={} error={}'.format(url_and_retry[0], e.__class__.__name__))
                self.proxy_pool.feedback_proxy(proxy, level=1)
                retry -= 1

        with self.thread_locks[tid]:
            self.threads_status[tid] = (res, url_and_retry)

    def scrap_done(self, res, url_and_retry):
        url = url_and_retry[0]
        if res is None:
            self.redis.srem(self.doing_key, url)
            if url in self.crawled:
                self.crawled.remove(url)
            if url_and_retry[1] < 3:
                self.add_job(url, url_and_retry[1] + 1)
            self.add_stats({'error': 1})
        else:
            self.finish_job(url)
            soup = BeautifulSoup(res.text, 'html.parser')
            try:
                self.parse(soup, url)
                self.q_stats.put({'success': 1})
            except KeyboardInterrupt:
                return
            except:
                self.log("Error occurs when parsing the content. ({})".format(url), 'ERR')
                self.q_log.put('Parsing Error: url={}'.format(url))
                self.q_stats.put({'error': 1})

    def add_job(self, url, retry_cnt=0):
        url = self.clean_url(url)
        if url not in self.crawled and \
                not self.redis.sismember(self.done_key, url) and \
                not self.redis.sismember(self.doing_key, url):
            self.crawled.add(url)
            self.redis.lpush(self.todo_key, (url, retry_cnt))
            self.q_stats.put({'pushed_urls': 1})

    def pop_job(self):
        try:
            url_and_retry = self.redis.brpop(self.todo_key, timeout=10)
            if url_and_retry is None:
                raise Empty
            url_and_retry = url_and_retry[1].decode("utf-8")
            if url_and_retry[0] != "(":
                url_and_retry = (url_and_retry, 0)
            else:
                url_and_retry = eval(url_and_retry)
            url_and_retry = (self.clean_url(url_and_retry[0]), url_and_retry[1])
            self.redis.sadd(self.doing_key, url_and_retry[0])
            return url_and_retry
        except Empty:
            return None

    def finish_job(self, url):
        self.redis.sadd(self.done_key, url)
        self.redis.srem(self.doing_key, url)

    def reset_task(self):
        self.redis.delete(self.done_key, self.doing_key, self.todo_key)

    def add_result(self, result):
        self.q_results.put(result)

    def add_stats(self, stats):
        self.q_stats.put(stats)

    def is_master(self):
        return self.rank == 0

    def log(self, msg, level='INFO'):
        print("| {} <Crawler>: {}".format(level, msg))
