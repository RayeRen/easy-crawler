from core.proxy_pool import ProxyPool, register_proxy_pool
from .haipproxy.client.py_cli import ProxyFetcher


@register_proxy_pool("haip")
class HaipProxyPool(ProxyPool):
    """
    https://github.com/SpiderClub/haipproxy
    """

    def __init__(self, redis_db, args=None):
        super().__init__(redis_db, args)
        self.fetcher1 = ProxyFetcher('http', strategy='greedy', redis_conn=redis_db)
        self.fetcher2 = ProxyFetcher('http', strategy='greedy', redis_conn=redis_db)

    def collect_proxies(self):
        for p in self.fetcher1.get_proxies():
            self.add_proxy(p)
        for p in self.fetcher2.get_proxies():
            self.add_proxy(p)
