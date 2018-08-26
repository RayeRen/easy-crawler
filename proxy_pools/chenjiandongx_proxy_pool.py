import os
import requests
from core.proxy_pool import ProxyPool, register_proxy_pool


@register_proxy_pool("chenjiandongx")
class ChenjiandongxProxyPool(ProxyPool):
    """
    https://github.com/chenjiandongx/async-proxy-pool

    Set the port 12345
    """

    def __init__(self, redis_db, args=None):
        super().__init__(redis_db, args)
        self.proxy_pool_host = os.environ.get('PROXY_POOL_SERVER_HOST', 'localhost')
        self.port = os.environ.get('CJDX_PORT', '3289')

    def collect_proxies(self):
        for p in [list(p.values())[0] for p in
                  requests.get(
                      "http://%s:%s/get/100000" % (self.proxy_pool_host, self.port)).json()]:
            self.add_proxy(p)
