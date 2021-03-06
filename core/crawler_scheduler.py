import collections
import datetime
import json
import os
import time
from copy import deepcopy
from multiprocessing import Process, Queue, Manager, Lock
from queue import Empty
import redis
import requests

from core.utils import start_thread
from .config import Config
from .proxy_pool import PROXY_POOL_REGISTRY, ProxyPool
import proxy_pools  # donnot move

requests.packages.urllib3.disable_warnings()


class CrawlerScheduler:
    def __init__(self, crawler_cls, task_name, qps=80,
                 proxy_pool=None, process_num=None, thread_num=None, **kwargs):
        MAX_QUEUE_SIZE = self.MAX_QUEUE_SIZE = 100000
        self.q_results = Queue(MAX_QUEUE_SIZE)
        self.q_stats = Queue(MAX_QUEUE_SIZE)
        self.q_log = Queue(MAX_QUEUE_SIZE)
        self.q_proxy_feedback = Queue(MAX_QUEUE_SIZE)

        self.PROXY_QUEUE_SIZE = 2000

        self.process_num = int(process_num or min(os.cpu_count(), 20))
        self.q_proxy = Queue(self.PROXY_QUEUE_SIZE)
        self.thread_num = int(thread_num)
        self.task_name = task_name
        self.restart = kwargs.get('restart', False)
        self.qps = qps
        if proxy_pool is None:
            self.log("No proxy pool is specified. Use fake proxy pool.", 'WARN')
            self.proxy_pool = "fake"
        else:
            assert proxy_pool in PROXY_POOL_REGISTRY, \
                self.log("{} is not registered.".format(proxy_pool), 'ERR', False)
            self.proxy_pool = proxy_pool
            self.log("Use {} proxy pool".format(proxy_pool))

        self.procs = []
        self.terminate = False
        self.crawler_cls = crawler_cls
        self.args = kwargs
        self.stats = collections.defaultdict(lambda: 0)
        manager = Manager()
        self.context = {}

        self.runtime_context = manager.dict()
        self.runtime_context['cur_max_threads_num'] = self.thread_num / 2
        self.runtime_context['terminate'] = False
        self.runtime_context['working'] = 0

        # redis
        rdp = redis.ConnectionPool(host=Config.REDIS_HOST,
                                   port=Config.REDIS_PORT, db=0,
                                   max_connections=1000)
        self.redis = redis.StrictRedis(connection_pool=rdp)
        self.todo_key = self.task_name + "_todo"
        self.doing_key = self.task_name + "_doing"
        self.done_key = self.task_name + "_done"
        self.RESET_FREEZE_SPEED_SEC = 30

        # proxy pool
        kwargs['task_name'] = task_name
        kwargs['repeat'] = 3
        self.proxy_pool = PROXY_POOL_REGISTRY[proxy_pool](self.redis, kwargs)

    def run(self):
        start_urls = self.crawler_cls.prepare(self.context, self.runtime_context, self.args)
        assert isinstance(start_urls, list), "Prepare method should return a list."

        self.proxy_pool.collect_proxies()
        self.proxy_pool.shuffle_proxies()
        self.log("Collect %d proxies." % self.proxy_pool.proxies.qsize())

        start_thread(self.collect_proxies)
        start_thread(self.feedback_proxy)
        start_thread(self.monitor)
        start_thread(self.collect_results)
        start_thread(self.collect_stats)
        start_thread(self.write_log)

        for i in range(self.process_num):
            self.procs.append(Process(
                target=CrawlerScheduler.run_single_process,
                args=(
                    self.task_name,
                    start_urls if i == 0 else [],
                    self.q_results,
                    self.q_stats,
                    self.q_log,
                    self.q_proxy,
                    self.q_proxy_feedback,
                    i,
                    self.crawler_cls,
                    self.thread_num,
                    self.restart,
                    self.runtime_context,
                    self.args,
                )))
            self.procs[i].start()
        try:
            for i in range(self.process_num):
                self.procs[i].join()
        except KeyboardInterrupt:
            self.terminate = True
            for proc in self.procs:
                proc.terminate()
            self.runtime_context['terminate'] = True
            raise KeyboardInterrupt

    @staticmethod
    def run_single_process(task_name, start_urls,
                           q_results, q_stats, q_log, q_proxy, q_proxy_feedback,
                           rank, crawler_cls, thread_num,
                           restart, shared_context, args):
        crawler = crawler_cls(task_name, start_urls,
                              q_results, q_stats, q_log, q_proxy, q_proxy_feedback,
                              rank, thread_num,
                              restart, shared_context, args)
        crawler.run()

    def collect_proxies(self):
        while not self.terminate:
            if self.q_proxy.qsize() < self.PROXY_QUEUE_SIZE - 50:
                self.q_proxy.put(self.proxy_pool.get_proxy())
            else:
                time.sleep(0.5)

    def feedback_proxy(self):
        while not self.terminate:
            if not self.q_proxy_feedback.empty():
                proxy, level = self.q_proxy_feedback.get()
                self.proxy_pool.feedback_proxy(proxy, level)
            else:
                time.sleep(0.5)

    def collect_stats(self):
        while not self.terminate:
            if not self.q_stats.empty():
                new_stats = self.q_stats.get(timeout=5)
                if new_stats is not None:
                    for k, v in new_stats.items():
                        self.stats[k] += v
            else:
                time.sleep(0.5)

    def collect_results(self):
        while not self.terminate:
            if not self.q_results.empty():
                result = self.q_results.get(timeout=5)
                self.crawler_cls.collect_results(self.context, result)
            else:
                time.sleep(0.5)

    def monitor(self):
        last_t = t = time.time()
        last_scraped = 0
        last_custom_monitor = {}
        dead = 0
        avg_speed = 0
        cnt = 0
        accmu_step = 5

        freeze_speed_sec = 100

        while not self.terminate:
            time.sleep(5)
            time_escape = int(time.time() - t)
            last_time_escape = time.time() - last_t
            stats = deepcopy(self.stats)
            last_t = time.time()
            stats.update({
                'time_escape(s)': int(time_escape),
                'new_total': stats['success'] - last_scraped,
                'speed (pages/sec)': round(stats['success'] / time_escape, 2),
                'todo_queue_size': self.redis.llen(self.todo_key),
                'cur_threads': self.runtime_context['cur_max_threads_num'],
                'bad_proxies': self.redis.scard(self.proxy_pool.bad_proxies_name),
                'proxies_queue_size': self.q_proxy.qsize(),
                'working': self.runtime_context['working'],
            })
            if dead > 5:
                stats.update({"dead": str(dead) + "/20"})

            real_speed = stats['real time speed (pages/sec)'] = round(stats['new_total'] / last_time_escape, 2)
            custom_monitor, terminate = self.crawler_cls.monitor(self.context, last_time_escape, last_custom_monitor)
            stats.update(custom_monitor)
            last_custom_monitor = custom_monitor

            if self.qps is None:
                self.runtime_context['cur_max_threads_num'] = self.thread_num
            else:
                freeze_speed_sec -= last_time_escape
                if cnt < accmu_step:
                    cnt += 1
                    avg_speed += real_speed
                else:
                    cnt = 0
                    avg_speed += real_speed
                    avg_speed /= accmu_step
                    if freeze_speed_sec < 0:
                        if avg_speed > self.qps + 15:
                            self.adjust_speed(increase=False)
                        elif avg_speed < self.qps - 15:
                            self.adjust_speed(increase=True)
                        freeze_speed_sec = self.RESET_FREEZE_SPEED_SEC
                    avg_speed = 0

            last_scraped = stats['success']

            # terminate when no task comes in.
            if stats['new_total'] == 0:
                dead += 1
                if dead > 100:
                    for proc in self.procs:
                        proc.terminate()
                    self.runtime_context['terminate'] = True
                    self.terminate = True
            else:
                dead = 0
            print(json.dumps(stats))

    def adjust_speed(self, increase=True):
        if increase:
            self.runtime_context['cur_max_threads_num'] = min(self.runtime_context['cur_max_threads_num'] * 1.1,
                                                              self.thread_num)
            self.log("Increase crawling speed.")
        else:
            self.runtime_context['cur_max_threads_num'] = max(self.runtime_context['cur_max_threads_num'] * 0.9, 10)
            self.log("Decrease crawling speed.")

    def write_log(self):
        os.makedirs("logs", exist_ok=True)
        with open("logs/{}_{}.log".format(
                self.task_name, datetime.datetime.now().strftime('%Y%m%d_%H_%M_%S')),
                'w', encoding='utf-8') as f:
            while True:
                log = self.q_log.get()
                f.write(log + "\n")

    def log(self, msg, level='INFO', should_print=True):
        s = "| {} <Scheduler>: {}".format(level, msg)
        if should_print:
            print(s)
        return s
