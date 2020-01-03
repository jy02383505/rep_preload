# from core.rediswrapper import RedisWrapper
from core.config import config
import redis

def getDB(db):
    pool = redis.ConnectionPool(host=config.get('redis', 'host'), port=6379, db=db,
                                password=config.get('redis', 'password'), decode_responses=True)
    r = RedisWrapper(connection_pool=pool)
    r.set_bredis(config.get('redis', 'host_bak'), db, password=config.get('redis', 'password'))
    return r


# get bermuda redis
def getBR(db):
    pool = redis.ConnectionPool(host=config.get('redis', 'host_bermuda'), port=6379, db=db,
                                password=config.get('redis', 'password_bermuda'), decode_responses=True) # 10.20.56.91
    br = RedisWrapper(connection_pool=pool)
    br.set_bredis(config.get('redis', 'host_bermuda_bak'), db, password=config.get('redis', 'password_bermuda')) # 10.20.56.87
    return br


class RedisWrapper(redis.Redis):
    def set_bredis(self, host, db, password):
        pool = redis.ConnectionPool(host=config.get('redis', 'host_bak'), port=6379, db=db, password=password)
        self.bredis = redis.Redis(connection_pool=pool)

    def exists(self, key):
        try:
            return super(RedisWrapper, self).exists(key)
        except Exception as e:
            return self.bredis.exists(key)

    def expire(self, key, CACHE_TIMEOUT):
        try:
            return super(RedisWrapper, self).expire(key, CACHE_TIMEOUT)
        except Exception as e:
            return self.bredis.expire(key, CACHE_TIMEOUT)

    def delete(self, key):
        try:
            return super(RedisWrapper, self).delete(key)
        except Exception as e:
            return self.bredis.delete(key)

    def get(self, key):
        try:
            return super(RedisWrapper, self).get(key)
        except Exception as e:
            return self.bredis.get(key)

    def set(self, key, value):
        try:
            super(RedisWrapper, self).set(key, value)
        except Exception as  e:
            self.bredis.set(key, value)

    def pipeline(self):
        try:
            pipe = super(RedisWrapper, self).pipeline()
            pipe.watch("test")
            pipe.execute()
            return pipe
        except Exception as e:
            return self.bredis.pipeline()

    def hvals(self, key):
        try:
            return super(RedisWrapper, self).hvals(key)
        except Exception as e:
            return self.bredis.hvals(key)

    def hkeys(self, hash_name):
        try:
            return super(RedisWrapper, self).hkeys(hash_name)
        except Exception as e:
            return self.bredis.hkeys(hash_name)

    def hset(self, hash_name, key_name, value):
        try:
            return super(RedisWrapper, self).hset(hash_name, key_name, value)
        except Exception as e:
            return self.bredis.hset(hash_name, key_name, value)

    def hmset(self, name, mapping):
        try:
            return super(RedisWrapper, self).hmset(name, mapping)
        except Exception as  e:
            return self.bredis.hmset(name, mapping)

    '''right push'''

    def rpush(self, name, *values):
        try:
            return super(RedisWrapper, self).rpush(name, values)
        except Exception as e:
            return self.bredis.rpush(name, values)

    '''right pop'''

    def rpop(self, name, *values):
        try:
            return super(RedisWrapper, self).rpop(name, values)
        except Exception as e:
            return self.bredis.rpop(name, values)

    def hexists(self, name, key):
        try:
            return super(RedisWrapper, self).hexists(name, key)
        except Exception as e:
            return self.bredis.hexists(name, key)