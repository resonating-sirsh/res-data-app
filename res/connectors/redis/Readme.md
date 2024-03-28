# Introduction to the redis connector

Redis is a convenient distributed key value store that we have added to the dev and prod Kubernetes cluster. To play with it locally you should use Kube Forwarder to open a port. The recommended thing to do is use the same port 6379 (redis default) for dev and prod and open for one of these at a time. On the cluster this is the port we use too.
To experiment you can connect to Dev. Later to inspect your production data you may also want to connect to Prod.

**Note** it is not particularly easy to query Redis in batch although you can lookup all keys. Its best used when you already know the keys you want to use or have already used in the past.

You can easily use Redis directly since it has a simple python API. Our Redis connector provides a very thin wrapper around Redis. For example it pickles and unpickles objects or it generates keys that represent schema and tables.
For example if you had a namespace called `MY_APP` and a table called `MY_CACHE` you could use the redis connector as follows

```python
import res
redis = res.connectors.load('redis')
my_key_value_cache = redis['MY_APP']['MY_CACHE']
my_key_value_cache['key'] = 'value'
```

**Note** When you use are using a dev environment, so that debug messages are shown, you will see that when keys are accessed in redis they take the form `SCHEMA:TABLE_:KEY` by the convention used in our connector. You own the keys in your App assuming no one uses the same names!

This data will be stored on either the dev or prod cluster. You can later access those data

```python
x = my_key_value_cache['key']
print(x)
```

You might also use a pattern where you want to save more complex data like dictionaries which is fine too

```python
my_key_value_cache['some_mapping'] = {'a':1, 'b':2}
```

And you can read and write from that.

Additionally there are some simple wrappers for updating sets and lists. The use cases here are you want to store a list or a set in the distributed store and then add things without checking what is there already.

```python
my_key_value_cache['my_list'] = [1,2,3]
my_key_value_cache.append_to_list( 'my_list', 4)
print(my_key_value_cache['my_list'])
```

or for sets

```python
my_key_value_cache['my_set'] = {1,2,3} #it works if use a set or a list here as it will be ensuring distinctness anyway
my_key_value_cache.append_to_set( 'my_set', 4)
print(my_key_value_cache['my_set'])

my_key_value_cache.append_to_set( 'my_set', 4)
print(my_key_value_cache['my_set'])

```

There are some other experimental features such as locking to avoid concurrency violations etc but they have not been greatly tested yet. You could however look into extending and test the connector if you have the use case.
