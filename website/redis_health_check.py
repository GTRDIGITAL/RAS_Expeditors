# import redis
# import os

# def check_redis_connection():
#     """
#     Checks the connection to the Redis server.
#     """
#     print("Checking Redis connection...")  # Added this line

#     try:
#         # WARNING: DO NOT USE THIS IN PRODUCTION.  This is only for local testing.
#         redis_password = "7QQn02l0LgNg9o2r6vttg0BInca7gEzkUAzCaCKUF8Y="  # Hardcoded password for testing
#         redis_host = 'RedisBoxGT.redis.cache.windows.net'
#         redis_port = 6380  # Changed to 6380

#         r = redis.Redis(
#             host=redis_host,
#             port=redis_port,  # Changed to 6380
#             password=redis_password,
#             ssl=True,  # Enable SSL
#             ssl_certfile=None,
#             ssl_keyfile=None,
#             ssl_cert_reqs='required'  # Verify the server's certificate
#         )

#         # Attempt to ping the Redis server
#         r.ping()
#         print("Successfully connected to Redis!")
#         return True

#     except redis.exceptions.ConnectionError as e:
#         print(f"Failed to connect to Redis: {e}")
#         return False
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         return False

# if __name__ == "__main__":
#     check_redis_connection()

import redis
import os
from dotenv import load_dotenv

load_dotenv()

REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_HOST = os.environ.get("CELERY_BROKER_URL").split('@')[1].split(':')[0]
REDIS_PORT = 6380

try:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True,
        ssl_certfile=None,
        ssl_keyfile=None,
        ssl_cert_reqs='required'
    )
    r.ping()
    print("Successfully connected to Redis!")
    r.set('test_key', 'test_value')
    print("Successfully set a test key!")
    value = r.get('test_key')
    print(f"Successfully retrieved the test key: {value.decode('utf-8')}")
except Exception as e:
    print(f"Failed to connect to Redis: {e}")