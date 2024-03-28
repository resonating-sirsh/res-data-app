from .. import DatabaseConnector, DatabaseConnectorTable, DatabaseConnectorSchema
import pandas as pd
import redis
import os
import res
from pickle import loads, dumps

REDIS = os.environ.get("REDIS_HOST", "localhost")


class EndPointKeyCache:
    """
    cache some things at endpoints in the API that are frequently accessed
    """

    def __init__(self, endpoint_name, ttl_minutes=60):
        self.name = endpoint_name
        self.ttl_seconds = ttl_minutes * 60
        self._cache: RedisConnector = res.connectors.load("redis")["ENDPOINT_CACHE"][
            endpoint_name
        ]

    @property
    def db(self):
        return self._cache._db

    def __getitem__(self, key):
        return self._cache[key]

    def __setitem__(self, key, value):
        key = self._cache._make_key(key)
        self.db.set(key, dumps(value), ex=self.ttl_seconds)
        return key


class RedisConnector(DatabaseConnector):
    def __init__(self):
        self._db = self.get_client()

    def get_client(self):
        """
        Get the quickbooks client object
        """

        conn = redis.StrictRedis(host=REDIS, port=6379)

        return conn

    def get_lock(self, key, timeout=10, blocking=False):
        """

        ACK a lock for some resource - with timeout - you should request enough time or fail and try without the lock if you need to

        import res
        import time
        redis = res.connectors.load('redis')
        LONG_ENOUGH_FOR_MUTATION = 5
        with redis.get_lock("LOCK:MAKE_SKU_COUNTER:ABCD",timeout=LONG_ENOUGH_FOR_MUTATION ) as f:
            print('ack')
            time.sleep(2)
            still_owned = f.owned()
            print(f'{still_owned=}')
            print('done')
        """

        return self._db.lock(key, timeout=timeout, blocking=blocking)

    def try_get_lock(self, key, timeout=10, blocking=False):
        try:
            lock = self.get_lock(key, timeout=timeout, blocking=blocking)
            lock.acquire()
            if lock.owned():
                return lock
        except:
            pass
        return None

    def __getitem__(self, schema):
        return RedisConnectorSchema(schema)


class RedisConnectorSchema(DatabaseConnectorSchema):
    def __init__(self, schema):
        self._schema = schema

    def __getitem__(self, table):
        return RedisConnectorTable(self._schema, table)


class RedisConnectorTable(DatabaseConnectorTable):
    def __init__(self, schema, table):
        """
        Constructor sets up the session for a particular account and endpoint
        """
        self._schema = schema
        self._table = table
        self._db = self.get_client()

    def get_lock(self, key, timeout=1000):
        k = self._make_key(key)
        return self._db.lock(key, timeout=timeout)

    def _make_key(self, key):
        return f"{self._schema}:{self._table}:{key}"

    def __getitem__(self, key):
        k = self._make_key(key)
        res.utils.logger.debug(k)
        o = self._db.get(k)
        return loads(o) if o is not None else None

    def __setitem__(self, key, value):
        k = self._make_key(key)
        res.utils.logger.debug(k)
        self._db.set(k, dumps(value))
        # res.utils.logger.debug(f"Added to redis with key {k}")

    def _str_lock(self, key):
        return self._db.lock(res.utils.res_hash(key.encode("utf-8")), timeout=10)

    def resolve_with_action(self, key, action):
        """
        we go via the cache for a value
        if the key does not exist, we lookup a secondary store, set the cache value and return
        for example in dgraph we can lookup for an object uid, and create or retrieve it in one transaction
        """
        with self._str_lock(key):
            v = self[key]
            if not v:
                v = action(self._table, key)
                self[key] = v
            return v

    def put(self, key, value, acquire_lock=True, retrieve=False):
        if not acquire_lock:
            self[key] = value
        else:
            with self._str_lock(key):
                self[key] = value
        return self[key] if retrieve else True

    def update_named_map(self, name, key, value):
        """
        A shared dictionary is a useful simple object to share state in a single map
        """
        with self._str_lock(name):
            data = self[name] or {}
            res.utils.logger.debug(f"Updating shared key {data}")
            data[key] = value
            # now out it back
            self[name] = data
            res.utils.logger.debug(f"Data are now {data}")
            return data

    def append_to_list(self, key, item):
        """
        acquire a lock and update a list
        """
        if not isinstance(item, list):
            item = [item]

        with self._str_lock(key):
            l = []
            l = self[key] or []

            l += item
            self[key] = l
            return l

    def add_to_set(self, key, item):
        """
        acquire a lock and update a set
        we actually coerce to lists first but respect sets or lists in the source
        """
        if isinstance(item, set):
            item = list(item)

        if not isinstance(item, list):
            item = [item]

        with self._str_lock(key):
            l = []
            l = self[key] or []
            if isinstance(l, list):
                l += item
            if isinstance(l, set):
                l |= set(item)
            self[key] = l
            return l

    def remove_from_set_if_exists(self, key, items):
        if not isinstance(items, list):
            items = [items]

        with self._str_lock(key):
            l = []
            l = self[key] or []
            l = [item for item in l if item not in items]
            self[key] = list(set(l))
            return l

    def read_table(self, keys):
        if not isinstance(keys, list):
            keys = [keys]

        res.utils.logger.debug(f"Reading cached table for {len(keys)} keys")
        data = [self[k] for k in keys]

        return pd.DataFrame([d for d in data if isinstance(d, dict)])

    def write_table(self, df, key_column, ack_lock=False):
        """
        Writes a batch of records which are treated like an indexed table
        you need to know the keys to retrieve
        """
        if df is None:
            return

        res.utils.logger.debug(f"Writing {len(df)} records to redis as table")

        for d in df.to_dict("records"):
            key = d[key_column]
            self[key] = d

        res.utils.logger.debug("records written to redis")

    def get_client(self):
        """
        Get the quickbooks client object
        """

        conn = redis.StrictRedis(host=REDIS, port=6379)

        return conn

    def delete(self, key):
        """
        Remove a key-value pair from the Redis database using the provided key.
        """
        k = self._make_key(key)
        if self._db.exists(k):  # Check if the key exists before trying to delete
            self._db.delete(k)
