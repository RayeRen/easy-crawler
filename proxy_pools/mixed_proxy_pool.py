import os

import redis
import requests

from core.proxy_pool import register_proxy_pool, ProxyPool
from .haipproxy.client.py_cli import ProxyFetcher


@register_proxy_pool("mixed")
class MixedProxyPool(ProxyPool):

    def __init__(self, redis_db, args=None):
        super().__init__(redis_db, args)
        self.proxy_pool_host = os.environ.get('PROXY_POOL_SERVER_HOST', 'localhost')
        rdp = redis.ConnectionPool(host=self.proxy_pool_host, db=0, max_connections=1000)
        self.redis = redis.StrictRedis(connection_pool=rdp)
        self.fetcher1 = ProxyFetcher('http', strategy='greedy', redis_conn=self.redis)
        self.fetcher2 = ProxyFetcher('https', strategy='greedy', redis_conn=self.redis)
        self.ports = {
            'jhao104': os.environ.get('JHAO104_PORT', '5010'),
            'karmenzind': os.environ.get('KARMEN_PORT', '12345'),
            'scylla': os.environ.get('SCYLLA_PORT', '8899'),
            'chenjiandongx': os.environ.get('CJDX_PORT', '3289'),
        }

    def collect_proxies(self):
        new_proxies = []
        self.log("Fetching haipproxy")
        new_proxies += self.fetcher1.get_proxies()
        new_proxies += self.fetcher2.get_proxies()  # https://github.com/SpiderClub/haipproxy

        self.log("Fetching jhao104")
        try:
            new_proxies += requests.get(
                "http://%s:%s/get_all/" % (
                    self.proxy_pool_host, self.ports['jhao104']),
                timeout=5).json()  # https://github.com/jhao104/proxy_pool
        except TimeoutError:
            self.log("jhao104 timeout", "ERR")

        self.log("Fetching scylla")
        try:
            new_proxies += [p['ip'] + ':' + str(p['port']) for p in
                            requests.get("http://%s:%s/api/v1/proxies" % (self.proxy_pool_host, self.ports['scylla']),
                                         timeout=5)
                                .json()['proxies']]  # https://github.com/imWildCat/scylla
        except TimeoutError:
            self.log("scylla timeout", "ERR")

        try:
            self.log("Fetching Karmenzind")
            new_proxies += [p['ip'] + ':' + str(p['port']) for p in
                            requests.get(
                                "http://%s:%s/api/proxy/?count=1000" % (self.proxy_pool_host, self.ports['karmenzind']),
                                timeout=5)
                                .json()['data']['detail']]  # https://github.com/Karmenzind/fp-server
        except TimeoutError:
            self.log("Karmenzind timeout", "ERR")

        self.log("Fetching chenjiandongx")
        # try:
            # ps = requests.get("http://%s:%s/get/1000" % (self.proxy_pool_host, self.ports['chenjiandongx']),
            #                   timeout=5).json()
            # new_proxies += [list(p.values())[0] for p in ps]  # https://github.com/chenjiandongx/async-proxy-pool
        # except TimeoutError:
        #     self.log("chenjiandongx timeout", "ERR")

        for p in new_proxies:
            self.add_proxy(p)
