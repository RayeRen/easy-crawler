import os
from dotenv import load_dotenv

try:
    load_dotenv(verbose=True)
except:
    print("| WARN: .env not found. You should copy the .env.example to .env first.")


class Config:
    REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
    REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
    PROXY_POOL_SERVER_HOST = os.environ.get('PROXY_POOL_SERVER_HOST', 'localhost')
