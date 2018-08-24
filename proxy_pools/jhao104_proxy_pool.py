import os
import requests
from core.proxy_pool import ProxyPool, register_proxy_pool


@register_proxy_pool("jhao104")
class JHao104ProxyPool(ProxyPool):
    """
    https://github.com/jhao104/proxy_pool
    """

    def __init__(self, redis_db, args=None):
        super().__init__(redis_db, args)
        self.proxy_pool_host = os.environ.get('PROXY_POOL_SERVER_HOST', 'localhost')
        self.port = os.environ.get('JHAO104_PORT', '5010')

    def collect_proxies(self):
        for p in requests.get(
                "http://%s:%s/get_all/" % (self.proxy_pool_host, self.port)).json():
            self.add_proxy(p)
