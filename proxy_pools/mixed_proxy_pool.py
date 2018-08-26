import os
import requests

from core.proxy_pool import register_proxy_pool, ProxyPool
from .haipproxy.client.py_cli import ProxyFetcher


@register_proxy_pool("mixed")
class MixedProxyPool(ProxyPool):

    def __init__(self, redis_db, args=None):
        super().__init__(redis_db, args)
        self.proxy_pool_host = os.environ.get('PROXY_POOL_SERVER_HOST', 'localhost')
        self.fetcher1 = ProxyFetcher('http', strategy='greedy', redis_conn=redis_db)
        self.fetcher2 = ProxyFetcher('https', strategy='greedy', redis_conn=redis_db)
        self.ports = {
            'jhao104': os.environ.get('JHAO104_PORT', '5010'),
            'karmenzind': os.environ.get('KARMEN_PORT', '12345'),
            'scylla': os.environ.get('SCYLLA_PORT', '8899'),
            'chenjiandongx': os.environ.get('CJDX_PORT', '3289'),
        }

    def collect_proxies(self):
        new_proxies = []
        new_proxies += self.fetcher1.get_proxies()
        new_proxies += self.fetcher2.get_proxies()  # https://github.com/SpiderClub/haipproxy

        new_proxies += requests.get(
            "http://%s:%s/get_all/" % (
                self.proxy_pool_host, self.ports['jhao104'])).json()  # https://github.com/jhao104/proxy_pool

        new_proxies += [p['ip'] + ':' + str(p['port']) for p in
                        requests.get("http://%s:%s/api/v1/proxies" % (self.proxy_pool_host, self.ports['scylla']))
                            .json()['proxies']]  # https://github.com/imWildCat/scylla

        new_proxies += [p['ip'] + ':' + str(p['port']) for p in
                        requests.get(
                            "http://%s:%s/api/proxy/?count=100000" % (self.proxy_pool_host, self.ports['karmenzind']))
                            .json()['data']['detail']]  # https://github.com/Karmenzind/fp-server

        new_proxies += [list(p.values())[0] for p in
                        requests.get(
                            "http://%s:%s/get/100000" % (self.proxy_pool_host, self.ports['chenjiandongx']))
                            .json()]  # https://github.com/chenjiandongx/async-proxy-pool

        for p in new_proxies:
            self.add_proxy(p)
