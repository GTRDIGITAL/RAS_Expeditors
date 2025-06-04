import os

broker_url = f"redis://:{os.environ.get('REDIS_PASSWORD', '7QQn02l0LgNg9o2r6vttg0BInca7gEzkUAzCaCKUF8Y=')}@RedisBoxGT.redis.cache.windows.net:6379/0"
result_backend = f"redis://:{os.environ.get('REDIS_PASSWORD', '7QQn02l0LgNg9o2r6vttg0BInca7gEzkUAzCaCKUF8Y=')}@RedisBoxGT.redis.cache.windows.net:6379/0"