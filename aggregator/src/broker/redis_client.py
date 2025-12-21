import os
import redis

REDIS_URL = os.getenv("BROKER_URL", "redis://broker:6379")

redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True
)
