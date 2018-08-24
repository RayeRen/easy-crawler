import os
import requests
from core.proxy_pool import ProxyPool, register_proxy_pool


@register_proxy_pool("karmenzind")
class KarmenzindProxyPool(ProxyPool):
    """
    https://github.com/Karmenzind/fp-server

    Set the port 12345
    """

    def __init__(self, redis_db, args=None):
        super().__init__(redis_db, args)
        self.proxy_pool_host = os.environ.get('PROXY_POOL_SERVER_HOST', 'localhost')
        self.port = os.environ.get('KARMEN_PORT', '12345')

    def collect_proxies(self):
        for p in [p['ip'] + ':' + str(p['port']) for p in
                  requests.get("http://%s:%s/api/proxy/?count=10000" % (self.proxy_pool_host, self.port))
                          .json()['data']['detail']]:
            self.add_proxy(p)
