import random
import threading
import time
from queue import Empty, Queue

import OpenSSL
import redis
import requests
from OpenSSL.SSL import WantReadError
from bs4 import BeautifulSoup
from requests.exceptions import ProxyError, SSLError
from urllib3.exceptions import ProtocolError

from .config import Config
from .crawler_scheduler import CrawlerScheduler
from .utils import start_thread

requests.packages.urllib3.disable_warnings()


class Crawler:
    """
    Abstract Crawler
    """

    def __init__(self, task_name, start_urls,
                 q_results, q_stats, q_log, q_proxy, q_proxy_feedback,
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
        self.threads_status = [(-1, None)] * self.max_thread_num  # -1: available
        self.thread_locks = [threading.Lock() for _ in range(self.max_thread_num)]
        self.crawled = set()
        self.q_results = q_results

        # requests
        self.user_agents = [
            line.strip() for line in open('resources/agents_list.txt', encoding='utf-8').readlines() if
            line.strip() != ""
        ]
        self.q_proxy = q_proxy
        self.q_proxy_feedback = q_proxy_feedback

        # stats and logs
        self.q_stats = q_stats
        self.q_log = q_log

        # local job
        self.local_jobs = Queue(self.max_thread_num)
        self.local_response = Queue(1000000)

    @property
    def base_url(self):
        raise NotImplementedError

    @staticmethod
    def prepare(context, runtime_context, args):
        """
        Do something preparation and return a list of start urls.
        Running in MAIN process, before all workers starting.
        :param context: store some variable shared in `collect_results`
        :param runtime_context: store some variable shared in runtime
        :param args: same as args passed to `start`
        :return: list of start urls
        """
        raise NotImplementedError

    def parse(self, runtime_context, soup, url):
        """
        You should do 2 steps here:
        1. Parse the html, extract useful information and save them by calling `add_result`
        2. Extract NEXT urls and save them by calling `add_url`, making the crawler continue running.

        You should NOT write any thread-UNSAFE code here, such as writing to a file. Instead, you should
        pass the result to `collect_results` by calling `add_result`.
        :param runtime_context: runtime context shared within processes
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

    @staticmethod
    def monitor(context, time_escape, last_stats):
        """

        :param context: some variables saved in `collect_results`
        :param time_escape: time escaped since last monitor
        :param last_stats: last stats return by the monitor
        :param dict stats: terminate: bool
        """
        return {}, False

    def handle_error(self, res):
        return 1

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
            url = "/" + url
        return url

    def run(self):
        if self.restart:
            if self.is_master():
                self.reset_task()
            else:
                time.sleep(3)

        if self.is_master():
            todo_urls = list(self.redis.smembers(self.doing_key))
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
                self.add_job(url, front=True)
        else:
            time.sleep(3)

        for tid in range(int(self.max_thread_num)):
            start_thread(self.scrape_thread)

        start_thread(self.schedule_job)

        while True:
            self.shared_context['working'] = self.local_jobs.qsize()
            if not self.local_response.empty():
                self.scrap_done(*self.local_response.get())
            else:
                time.sleep(0.5)

    def schedule_job(self):
        while True:
            job = self.pop_job()
            if job is None:
                time.sleep(3)
                continue
            self.local_jobs.put(job)

    def scrape_thread(self):
        while True:
            if not self.local_jobs.empty():
                url_and_retry = self.local_jobs.get()
                self.scrape(url_and_retry)
            else:
                time.sleep(0.5)

    def scrape(self, url_and_retry):
        res = None
        retry = 10
        while retry > 0:
            proxy = self.q_proxy.get()
            if proxy is not None:
                proxies = {'https': proxy, 'http': proxy}
            else:
                proxies = None
            try:
                headers = {'User-Agent': random.choice(self.user_agents)}
                res = requests.get(
                    self.base_url + url_and_retry[0],
                    proxies=proxies, headers=headers, timeout=5 + 2 ** url_and_retry[1]
                )
                if res.status_code == 200:
                    self.q_proxy_feedback.put((proxy, 0))
                    break
                else:
                    self.q_proxy_feedback.put((proxy, 1))
                    self.q_log.put('Status_code Error: url={}, code={}'.format(url_and_retry[0], res.status_code))
                    retry -= self.handle_error(res)
                    res = None
            except ProxyError:
                self.q_proxy_feedback.put((proxy, 2))
                retry -= 1
                self.q_log.put('Proxy Error: url={}'.format(url_and_retry[0]))
            except (requests.exceptions.RequestException,
                    SSLError, OpenSSL.SSL.Error, WantReadError, ProtocolError) as e:
                self.q_proxy_feedback.put((proxy, 1))
                retry -= 1
                self.q_log.put('Connection Error: url={} error={}'.format(url_and_retry[0], e.__class__.__name__))

        self.local_response.put((res, url_and_retry))

    def scrap_done(self, res, url_and_retry):
        url = url_and_retry[0]
        if res is None:
            self.redis.srem(self.doing_key, url)
            if url in self.crawled:
                self.crawled.remove(url)
            if url_and_retry[1] < 3:
                self.add_job(url, url_and_retry[1] + 1)
            else:
                self.add_stats({'discarded_jobs': 1})
                self.q_log.put('Discard url: {}'.format(url))
            self.add_stats({'error': 1})
        else:
            self.finish_job(url)
            soup = BeautifulSoup(res.text, 'html.parser')
            try:
                self.parse(self.shared_context, soup, url)
                self.q_stats.put({'success': 1})
                self.q_log.put("success: {}".format(url))
            except KeyboardInterrupt:
                return
            except Exception as e:
                self.log("Error occurs when parsing the content: {} ({})".format(str(e), url), 'ERR')
                self.q_log.put('Parsing Error: url={}'.format(url))
                self.q_stats.put({'error': 1})

    def add_job(self, url, retry_cnt=0, front=False):
        url = self.clean_url(url)
        if url not in self.crawled and \
                not self.redis.sismember(self.done_key, url) and \
                not self.redis.sismember(self.doing_key, url):
            self.crawled.add(url)
            if front:
                self.redis.rpush(self.todo_key, (url, retry_cnt))
            else:
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
