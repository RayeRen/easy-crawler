# Easy Crawler (beta)
A simple, stable, scalable and swift general web crawler.

## Features
1. `Simple` less than 5 core classes.
2. `Swift` 100+ qps (requests per second) on avg.
3. `Pluggable Dynamic Proxy Pool` You can choose your own proxy pool freely, only need to add a few lines code.
4. `Adaptive Traffic Control` Easy Crawler will control the scraping speed to fit your setting.
5. `High Scalability` Easy to deploy as distributed crawlers.

## Dependencies

- Python 3+
- Redis Server


## Architecture
```
    <Easy Crawler> ---- (1) get proxy IP --- <Proxy Pool>
            |
            |
 (2) pull and (3) push tasks
            |
            |
    <Redis queue> ($REDIS_HOST:$REDIS_PORT) 
```

## Quick Start 

```bash

# 1. Install requirements:
pip install -r requirements

# 2. Copy the .env:
cp .env.example .env

# 3. Modify the .env:
vi .env

# 4. Start redis server
sudo apt install redis-server # If you haven't installed redis-server
redis-server

# 5. Start proxy pool server
# This example doesn't need a proxy pool server. It will use a fake proxy pool. For a crawler with real proxy pool, you can jump to `Build-in Proxy Pool` below for reference.

# 6. Run the Minimal Crawler

# HINT: You must run these scripts in the root directory of REPO
# linux or mac
export PYTHONPATH=. && python crawlers/simple_crawler.py
 
# windows
set PYTHONPATH=.
python python crawlers/simple_crawler.py

```

### Run the Glosbe Crawler

[Click Me](crawlers/glosbe/readme.md) 


## Custom Crawler

1. Run the `simple_crawler` example in the `Quick Start` to check if the custom crawler foundation works.

2. `cp crawlers/simple_crawler.py crawlers/YOUR_crawler.py`

3. Modify the code in `YOUR_crawler.py` after reading the interface comments carefully.

4. Run and enjoy your own crawler.

```bash
# linux or mac
export PYTHONPATH=. && python crawlers/YOUR_crawler.py 

# windows
set PYTHONPATH=.
python crawlers/YOUR_crawler.py 
```

## Built-in Proxy Pool

1. Install the proxy pool servers according to the guidance in their REPOs. 
2. Set the port in `.env`

### Haip Proxy Pool
> Repo: https://github.com/SpiderClub/haipproxy
- Proxy Name: `haip`

### Jhao104 Proxy Pool
> Repo: https://github.com/jhao104/proxy_pool
- Proxy Name: `jhao104`
- Port: $JHAO104_PORT

### Karmenzind Proxy Pool
> Repo: https://github.com/Karmenzind/fp-server
- Proxy Name: `karmenzind`
- Port: $KARMEN_PORT

### Scylla Proxy Pool
> Repo: https://github.com/imWildCat/scylla
- Proxy Name: `scylla`
- Port: $SCYLLA_PORT

### Chenjiandongx Proxy Pool
> Repo: https://github.com/imWildCat/scylla
- Proxy Name: `scylla`
- Port: $CJDX_PORT

### Mixed Proxy Pool
> Mix all of above pools together.
- Proxy Name: `mixed`
- Port: $JHAO104_PORT, $KARMEN_PORT, $SCYLLA_PORT

### Fake Proxy Pool
> Not use proxy.
- Proxy Name: `fake`


## Custom Proxy Pool

1. Create a `YOUR_PROXY_POOL.py` in `proxy_pools`.
2. Add a `YOUR_PROXY_POOL` class, which should extend `core.proxy_pool.ProxyPool`. Don't forget to add a `@register_proxy_pool("YOUR_PROXY_POOL_NAME")` decorator to your class.
3. Implement `collect_proxies`. You can override `get_proxy` and `feedback_proxy` if necessary.
4. Run your crawler with `proxy_pool="YOUR_PROXY_POOL_NAME"`
 
```python
# crawlers/YOUR_Crawler.py
if __name__ == "__main__":
    YOUR_Crawler.start(
        task_name="YOUR_crawler",
        proxy_pool="YOUR_PROXY_POOL_NAME",
        ...
    )
# Run `python crawlers/YOUR_Crawler.py` to test your proxy pool.
```

## Distributed Deployment

Just run crawlers with same `task_name` in each container. They will share the job queue in redis.

## Dockerize

Todo.
 
## Author
Yi Ren (RayeRen)
