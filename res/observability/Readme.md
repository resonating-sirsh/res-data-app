# Res Observability uses AI Log tracing to keep an eye on things

## Getting started quickly

To quickly try the search and agent tools, you can pick one of the existing stores such as the Styles store.

**Note:** you should use kube-forwarder to connect to the **production** instance of redis if you want to try the EntityStore with the agent or you can just use the Columnar data store which is backed by parquet files only.

```python
from res.observability.queues import Body, Style, ProductionRequest, Order
from res.observability.io import ColumnarDataStore, EntityDataStore

style_store = ColumnarDataStore(Style, description="a store for asking questions about the apply color queue")
```

The ColumnarStores use DuckDB to run SQL queries on parquet files. So any parquet file you save on S3 or elsewhere is enough to use this. But the `ColumnarDataStore` and the Pydantic Models use some convenient conventions. For example you can take any of the entity objects including the abstract base and create a model from it - by choosing a _namespace_ and _name_ you determine where on S3 the parquet file will be stored. Suppose you want to test the styles but you want to use your own file location...

```python
Model = Style.create_model("MyStyle", namespace="test")
#now create a store from this...
store = ColumnarDataStore(Model, description="me testing a store")
#this will be saved in a location ../namespace/name/.. 
store.add(Model**{ <DATA> }, key_field='name')
#add data to the store with store.add(new_records, key_field='name')
```

If you want to just query data in the existing styles store you can

```python
store = ColumnarDataStore(Style)
store('ask questions')
```

To preview the data you can load the Polars dataframe of the parquet data

```python
store.load()
```

There is also a little Polars magic that allows you to query the data with SQL - use the entity name for the table name;

```python
store.query("SELECT * from Style LIMIT 1;")
store.query("SELECT name, node, last_updated_at from Style LIMIT 1;")

```

You can also create stores from some random data e.g. a dataframe. This is good if you want to quickly create test models with test schema and data to see how the agents work.

```python
from res.observability.entity import AbstractEntity
#get a dataframe somewhere
df = ...
NewModel = AbstractEntity.create_model_from_data('test', df, namespace='test')
#you can delete the file on s3 as you experiment and change schema
```

If you use any of these methods, you can the test the ability to turn questions into queries with `store('ask some question')` which uses an LLM internally to turn the question into SQL. This will generate SQL and simply run the query against the store to return tabular data. However, this store is normally used inside an agent so you can try this by getting an agent from the store as shown below.
Sometimes the queries generated are not going to be the ones you intend or may not even work at all. You can learn to choose good data schema that help the agent out. Failing that, we can improve the prompting on the columnar data store in future so it copes better over many cases. However, the agent can do pretty well on a first attempt or by trial and error in many instances.

```python
agent = store.as_agent()
agent('ask a question about your data')
```

**Note** *The columnar data store loads field names and also "enums" i.e. distinct values of columns if there are fewer than say 50 unique values in the column. This can be very helpful to prompt the agent about how to use the data before actually querying the data!*

The agent uses OpenAI GPT 4 turbo which at the time of writing can be a bit slow to respond - but its worth experimenting with.

#### Redis and the entity store

Note that above we also recommended connecting to Redis. This is because we keep a store of entity keys and data for easy lookup of bodies, styles, orders and ONEs by identifiers. You can test this by getting a reference to an entity store.

```python
#this requires open connection to Redis on (prod) Kubernetes which you can do via kube-forwarder
estore = EntityDataStore(AbstractEntity)
estore['KT-2011']
```

Also, any agent has a store internally for its use e.g. `agent._entity_store['KT-2011']` which references that same Redis instance.

With that you should be able to get started quickly. Now we can dive into more details...

# ONE.AI

ONE AI uses an Interpreter Agent loop with a single prompt (plan) to search for functions to answer any question. Rather than burn time on building different types of agents and prompts, in this approach we shift focus to defining your functions and data.

The agent can load a handful of utility functions to learn about Resonance, lookup functions etc but mostly the work is in developing a functional model to search for data and solve problems.

We start with Pydantic types to define entities. We then create stores that can load vector or columnar data. Each store has a search function.

Functions are the core building blocks. By adding proper doc strings and typing information we can take ANY function and make it available to the LLM.

We have the following types of functions

1. Vector/Columnar search (also entity lookups)
2. API calls
3. flows - res-data function calls

Each of these functions are "indexed" i.e. their descriptions are stored in a vector store and can be searched. That means for any context/question, the LLM can select just the functions it needs and it can continue to revise functions as it generates answers.

In the following, we explain how Pydantic is used to create entities and then how functions are indexed. We will then give examples of creating functions that use vector or other data stores or make API calls.

## Entities

Entities are defined as basic types `AbstractEntity` or its subclass `AbstractVectorStoreEntity`. The vector store type indicates what embedding it uses - by default this is the OpenAI one since we can use it as a service and it requires no extra hardware. You can create new entities from these ones. But why and when will you want to do that?

### Creating your first entity

Stores are located on S3. Vector Stores are in the `lance` format while columnar stores are in the `parquet` format and queryable with `DuckDB` or `Polars`. All databases are "embedded" making it easy to add new data into the LLM memory. You might prefer using other sources of data such as Snowflake and Airtable and you can create your own functions to do that. It is convenient however to use the _embedded_ data paradigm to experiment and arguably to build powerful flexible agent systems. The first reason to create a custom type is to point to the physical location which by convention is stored at `/namespace/entity` under some store root.

These defaults have been set

```python
DEFAULT_HOME = "s3://res-data-platform/stores/"
STORE_ROOT = os.environ.get("RES_STORE_HOME", DEFAULT_HOME).rstrip("/")
VECTOR_STORE_ROOT_URI = f"{STORE_ROOT}/vector-store"
COLUMNAR_STORE_ROOT_URI = f"{STORE_ROOT}/columnar-store"
```

Entities are Pydantic Types. You can create a Pydantic type manually and use it to control the stores. Often however it is enough to generate a type dynamically. For example you can create a subclass of the vector store base class and just give the derived model a name and namespace. You can then ingest data using that generic schema.

```python
from res.observability.entity import  AbstractVectorStoreEntry
from res.observability.io import VectorDataStore
Model = AbstractVectorStoreEntry.create_model(name='test', namespace='default')
store = VectorDataStore(Model)

record = Model(name="name1", text="some text")
store.add(record)
```

This creates a vector (embedding) of the text and inserts it into  LanceDB store. The location would be `<VECTOR_STORE_ROOT_URI>/<namespace>/<name>`. Columnar data can be created in a similar way.

### Using stores

You can open any store if you have an entity/model. There are various wrappers that allow you to open by name etc.

```python
from res.observability.io import ColumnarDataStore
store = ColumnarDataStore.open(name='style' namespace='meta')
```

Once you have a store you can load it with `.load()` or do vector searches or sql searches on it. You can also turn it into an agent to query it with natural language

```python
agent = store.as_agent()
agent("What styles are currently in Export Color Pieces")
```

We will learn more about this below. This agent will run using a single plan. It is given a few bootstrapping functions but can be invoked with other functions. In this case above, the store is providing itself as a function which the interpreter will use by default. If it cannot answer the question with this store/function, it can lookup other functions to try and answer the question. It is assumed in the context illustrated here that the user knows the question can be answered with this store if at all.

### Indexing stores

Each store is added to a FunctionIndex vector store. The function's brief description is stored (as a vector embedding) so that later the agent can search for functions. In the case of Vector Stores we also sample from the data in the vector store and add it to the text that describes how the function can be used. For example the store could provide data about Resonance Standard Operating Procedures or conversations in a given slack channel.
There is an indexing utility that will loop over stores and add stores as indexed functions but any function can be added to the index.

It is important to note that there is a standard format to describe functions to OpenAI. A helper function can be used to describe any function that is properly commented with Google style doc strings. For example consider the function

```python
    def run_search(
        self,
        question: str,
        limit: int = 200,
    ):
        """
        Perform the columnar data search for the queries directly on the store. This store is used for answering questions of a statistical nature about the entity.

        **Args**
            question: supply a question about data in the store
            limit: limit the number of data rows returned - this is to stay with context window but defaults can be trusted in most cases
        """
```

We can create a function description using the utility with this signature

```python
#from res.observability.dataops import FunctionFactory, FunctionDescription, typing, describe_function
#you can call this for any python function to generate an object that can provide the function description that OpenAI uses
def describe_function(
    function: typing.Callable,
    add_sys_fields=False,
    augment_description=None,
    factory: FunctionFactory = None,
) -> FunctionDescription:
```

So for example

```python
function_description = describe_function(run_search)
fdict = function_description.function_dict()
#> 
{'name': 'run_search',
 'description': 'Perform the columnar data search for the queries directly on the store. This store is used for answering questions of a statistical nature about the entity.\nProvides context about the type [style] - ask your full question as per the function signature\n',
 'parameters': {'type': 'object',
  'properties': {'question': {'type': 'string',
    'description': 'supply a question about data in the store'},
   'limit': {'type': 'integer',
    'description': 'limit the number of data rows returned - this is to stay with context window but defaults can be trusted in most cases'}}}}
```

This is important. The whole approach relies on being able to build these description to guide the LLM. Some apparatus in res-data is provided to generate these things, store, search and load them as function that the LLM can use. Therein lies all the magic.

### Indexing API Functions

Suppose you wish to add a REST API function to the available functions. For example from the [Meta-ONE API](https://data.resmagic.io/meta-one/docs). You need to

- access the json file for the openai spec e.g [https://data.resmagic.io/meta-one/openapi.json](https://data.resmagic.io/meta-one/openapi.json)
- then select the endpoint as it appears e.g. `/meta-one/bodies/points_of_measure`

```python
from res.observability.io.index import index_api_function
#the json can be inferred for res-data apis by convention
#the endpoint should be literally what is shown in the swagger docs for the endpoint
index_api_function(endpoint='/meta-one/bodies/points_of_measure', 
                   schema_file_json='https://data.resmagic.io/meta-one/openapi.json'),
                   #you can preview the function description without saving the index by setting plan=True
                   plan=False
```

Now you can use this function via the agent...

```python
from res.learn.agents import InterpreterAgent
agent = InterpreterAgent()
agent('Please use a suitable api function to provide the points of measure for the size LG for body KT-2011')
#or debug intent
#agent("what function would you use to get body points of measure")
```
