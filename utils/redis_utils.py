import redis
from read_config import CONFIG

class RedisUtils:

    @staticmethod
    def get_buzzbreak_event_redis_client(decode_responses=False):
        host = CONFIG['buzzbreak_event_redis']['host']
        port = CONFIG['buzzbreak_event_redis']['port']
        return redis.Redis(host=host, port=port, decode_responses=decode_responses)


event_redis_client = RedisUtils.get_buzzbreak_event_redis_client()

