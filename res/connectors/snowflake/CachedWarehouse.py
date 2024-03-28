import res
import pandas as pd
from pandas.util import hash_pandas_object


class CachedWarehouse:
    """
    This is a wrapper around snowflake to use it as a cached look up of things we eventually want for analytics too

    Stores in snowflake ENV_SCHEMA.LU.TABLE
    """

    def __init__(self, name, key, df=None, klass=None, **kwargs):
        """

        supply a name and key for the cache
        new data can be added by a dataframe and later merged/written

        """
        env = "development"  # later we will read from os
        # what to check use cases - we can keep this data in development for now
        snowflake_env = "IAMCURIOUS_DEVELOPMENT"
        self._key = key

        self._name = name
        # self._fq_name = f""
        self._data = df
        self._table_name = f"""{snowflake_env}."{name}" """

        self._warehouse = res.connectors.load("snowflake")
        self._cache = res.connectors.load("redis")["cache"][name]
        self._missing_data_resolver = kwargs.get("missing_data_resolver")

    def read(self, keys=None):
        """
        Read the entire table or filter by keys
        (Redis) cache is only used when keys are provided
        """
        if keys:
            pass

        Q = f"""SELECT * from {self._table_name} """
        return self._warehouse.execute(Q)

    def write(self, update_cached_values=False):
        """
        Any values whos mdf hash does not match what was read from the database can be written now
        we look the cache whenever we write to it to cavoid concurrency violations

        returns the number of new values written

        TODO: implement update cached values
          what this will do is write "through" the cache because it will observe that the cached keys may be out of date if the warhouse is out of date

        """

        # what would the new hash be
        new_hash = hash_pandas_object(self._data.astype(str))

        if "row_hash" not in self._data:
            self._data["row_hash"] = None

        # check any data we have against the new hash
        data = self._data[self._data["row_hash"] != new_hash]
        if len(data):
            data = data.reset_index().drop("index", 1)
            if update_cached_values:
                # TODO: want to be careful how we do this
                pass

        # now make sure there is a row hash to write
        data["row_hash"] = data["row_hash"].fillna(hash_pandas_object(data.astype(str)))

        # the indexed df is used to store information about the
        # dump to redis cache

        # create table if not exists - noeed to refactor a little as im using qualified and unqulaified table names in different places
        self._warehouse.upsert(data, table=self._name, key=self._key)

    def get_keys(self, cached_only=False):
        """
        We should really be able to check the cache here but not sure...
        Keep in mind the warehouse does not necessarily have everything
        """

        Q = f"""SELECT "{self._index_name}" from {self._table_name} """
        data = self.execute(Q)
        keys = []
        if len(data):
            keys = list(data[self._index_name].unique())

        return keys

    def missing_keys_from_set(self, keys):
        known_keys = self.get_keys()
        return list(set(keys) - set(known_keys))

    def lookup_keys(self, keys):
        return self._lookup(keys)

    def __getitem__(self, key):
        return self._lookup(key)

    def refresh_cache(self):
        """
        for some key lookups the dimensions are static
        but for changing dimensions we may need to invalidate the cache
        """
        pass

    def _lookup(self, keys):
        """
        Does a merged cache lookup
        Check the cache first
        Merge into the request anything from the warehouse that is missing
        Write missing into cache for future
        return union
        """

        if not isinstance(keys, list):
            keys = [keys]

        # read the dataframe from redis
        data = self._cache.read_table(keys)
        cached_keys = (
            list(data[self._key].unique()) if data is not None and len(data) else []
        )
        missing = list(set(keys) - set(cached_keys))

        if len(missing):
            res.utils.logger.debug(
                f"Missing {len(missing)} keys - fetching from warehouse and adding to cache"
            )
            l = missing
            if isinstance(keys[0], str):
                l = [f"'{c}'" for c in l]
            l = ",".join([str(s) for s in l])
            Q = f"""SELECT * FROM {self._table_name} WHERE "{self._key}" in ({l})  """
            added_data = self._warehouse.execute(Q)

            # if its still missing we need a resolver "airtable"
            missing = list(set(keys) - set(added_data[self._key].values))
            if len(missing):
                # TODO: have not tested this yet - assume we have the universe already
                res.utils.logger.debug(
                    f"Need to use the resolver to find missing - you will need to supply a resolver(missing_keys) in the constructor if you have not for resolving keys"
                )
                event_more_added_data = self._missing_data_resolver(missing)
                # todo, we can reload the warehouse with the missing data but now its in the cache anyway and it will be loaded to warehouse nightly one assumes?
                # we dont trust the missing data resolver so we drop dups on the key that we know and reset indexes
                added_data = (
                    pd.concat([added_data, event_more_added_data])
                    .drop_duplicates(subset=[self._key])
                    .reset_index()
                )

            # write the dataframe to redis
            self._cache.write_table(added_data, key_column=self._key)
            data = pd.concat([data, added_data]).reset_index()

        return data
