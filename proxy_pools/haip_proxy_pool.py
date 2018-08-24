from core.proxy_pool import ProxyPool, register_proxy_pool
from .haipproxy.client.py_cli import ProxyFetcher


@register_proxy_pool("haip")
class HaipProxyPool(ProxyPool):
    """
    https://github.com/SpiderClub/haipproxy
    """

    def __init__(self, redis_db, args=None):
        super().__init__(redis_db, args)
        self.type1_fetcher = ProxyFetcher('https', strategy='greedy', redis_conn=redis_db)

    def collect_proxies(self):
        for p in self.type1_fetcher.get_proxies():
            self.add_proxy(p)
