from core.proxy_pool import ProxyPool, register_proxy_pool


@register_proxy_pool("fake")
class FakeProxyPool(ProxyPool):

    def __init__(self, redis_db, args):
        super().__init__(redis_db, args)

    def collect_proxies(self):
        pass

    def get_proxy(self):
        return None
