import os
import requests
from core.proxy_pool import ProxyPool, register_proxy_pool


@register_proxy_pool("scylla")
class ScyllaProxyPool(ProxyPool):
    """
    https://github.com/imWildCat/scylla
    """

    def __init__(self, redis_db, args=None):
        super().__init__(redis_db, args)
        self.proxy_pool_host = os.environ.get('PROXY_POOL_SERVER_HOST', 'localhost')
        self.port = os.environ.get('SCYLLA_PORT', '8899')

    def collect_proxies(self):
        for p in [p['ip'] + ':' + str(p['port']) for p in
                  requests.get("http://%s:%s/api/v1/proxies" % (self.proxy_pool_host, self.port)).json()['proxies']]:
            self.add_proxy(p)
