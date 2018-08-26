import random
from collections import Counter
from queue import Queue

PROXY_POOL_REGISTRY = {}
PROXY_POOL_CLASS_NAMES = set()


def register_proxy_pool(name):
    """Decorator to register a new proxy pool."""

    def register_pool_cls(cls):
        if name in PROXY_POOL_REGISTRY:
            raise ValueError('Cannot register duplicate proxy pool ({})'.format(name))
        if not issubclass(cls, ProxyPool):
            raise ValueError('proxy pool ({}: {}) must extend ProxyPool'.format(name, cls.__name__))
        if cls.__name__ in PROXY_POOL_CLASS_NAMES:
            raise ValueError('Cannot register proxy pool with duplicate class name ({})'.format(cls.__name__))
        PROXY_POOL_REGISTRY[name] = cls
        PROXY_POOL_CLASS_NAMES.add(cls.__name__)
        return cls

    return register_pool_cls


class ProxyPool:
    REDIS_BAD_PROXY = "proxy_pool@bad_proxy"

    def __init__(self, redis_db, args=None):
        self.args = args or {}
        self.redis = redis_db
        self.proxies_list = []
        self.proxy_retry = Counter()
        self.proxies = Queue(100000)
        self.bad_proxies = self.redis.smembers(self.REDIS_BAD_PROXY)
        self.first = True

    def collect_proxies(self):
        raise NotImplementedError

    def shuffle_proxies(self):
        random.shuffle(self.proxies_list)
        for p in self.proxies_list:
            self.proxies.put(p)

    def feedback_proxy(self, proxy, level=0):
        if level == 0:
            self.proxies.put(proxy)
            self.proxy_retry[proxy] = 0
        elif level == 1 or level == 2:
            self.proxy_retry.update(proxy)
            if self.proxy_retry[proxy] > 3:
                if level == 2:
                    self.redis.sadd(self.REDIS_BAD_PROXY, proxy)
            else:
                self.proxies.put(proxy)

    def get_proxy(self):
        """
        get a proxy.
        :return: proxy
        """
        if self.proxies.empty():
            self.collect_proxies()
            self.log('No proxy available! Recollect.', 'WARN')

        proxy = self.proxies.get(timeout=5)
        while self.redis.sismember(self.REDIS_BAD_PROXY, proxy):
            proxy = self.proxies.get(timeout=5)
        return proxy

    def add_proxy(self, proxy):
        if not proxy.startswith("http"):
            assert isinstance(proxy, str), "Proxy <{}> is not a str".format(proxy)
            proxy = "http://" + proxy

        if proxy not in self.bad_proxies:
            self.proxies_list.append(proxy)

    def log(self, msg, level='INFO'):
        print("| {} <ProxyPool>: {}".format(level, msg))
