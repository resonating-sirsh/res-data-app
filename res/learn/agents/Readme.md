# How does ASK.ONE Work?

This readme describes the research done so far on creating a chat interface over our data. The approach taken here is just one of many possibilities that we can build on.

The key ideas that will be discussed are as follows

1. Function calling is a core aspect. Most of the effort has been in creating a data layer for the agent to use in terms of functions
2. The `InterpreterAgent` class is the main agent loop - it relies heavily on function calling
3. Adding the agent to slack and making use of slack as an interface to ingest various content and ask questions

# 0 Quick Start

If you want to ignore the theory until later, you can import the agent as follows and play with it

```python
from res.learn.agents import InterpreterAgent
agent = InterpreterAgent()

#redis ports needed: if using the entity store you need to first kube forward to the production redis 
agent('ask something about resonance')

#bypass the more opinionated prompting and agent loop to just call out to open ai
#agent.ask('')

```

# 1 The Interpreter Agent

The first generation of agents built in `res-data` used Langchain and its tools paradigm. When OpenAI released function calling, Lanchain was thrown out of the res-data picture since the function interface is a great way to abstract tools and allow an agent to call out to functions and data.
The idea was to imagine an infinite processing loop where an agent could modify (a) its function stack and (b) its in-memory context. At this time GPT had a much smaller context window. Now, with the 128k limit, although large context is not a panacea, i have invested less time in worrying about context length in the short term.

The Interpreter Agent approach assumes that all problems can be solved as an infinite loop of agent function calls where it is assumed context changes with each function call. I have explored other approaches like the PlannerAgent where we first make a Plan over all functions to answer the question and the set of workers to run the plan. This has been difficult to guide (in general) but is also interesting to consider for future.

you will notice in the main agent loop i.e. the `run` method that we loop continuously, up to some limit parameter or until the Open AI interface sends a stop instruction. This is supposed to happen when it has gathered context and is ready to answer the question.

**Note** _An unsolved problem at present is that sometimes the model merely constructs and states its plan and then terminates without actually executing the plan. Sometimes this may be due to excessive token usage in the sense that it feels it will not complete or else some random behaviour of the model. There is some observation in the last several months that in the case of generating large outputs, GPT has become lazy since it is being overworked by perhaps even lazier humans getting it to do their work._

## 1.1 Revising functions

If you look at the core block in the `run` function you will see that the call to open ai `openai.chat.completions.create` takes a list of functions as a parameter. The trick here is to continue to _revise_ those functions as we address the users request.

We start the agent with some initial functions such as a function to look into the Resonance context i.e. an "about resonance" function. This could well search a database but here it just returns a blurb. Another very important function is the `revise_functions` function because this will take a context i.e. a question and do a vector search for functions (stores) that could help to answer the question. Matched functions are loaded onto the function stack for calling.

The agent loop runs up to some limit e.g. 10 iterations, and will continue to try to solve the user's question, querying data (which is added to context), possibly revising functions again to query more data and so on.

Note: _Some future work is planned to think about how the function calling can be done in parallel e.g. using something like Ray to run a query against each matched store in parallel and return all data possibly with some re-ranking_

Inside the agent loop, we make use of OpenAI function calling; OpenAI will send a response to say what function to call and with which arguments to call the function. We use a thin `invoke` wrapper to call functions (see more on functions below) as we iterate inside the agent loop.

## 1.2 The Global prompt

In the early days of LLM use, prompting became (and still is) very prominent. Complex systems require complex prompting and people managed prompt libraries and strategies etc. While I played with this idea (for countless hours), I found it brittle and unsatisfying. The approach I take here is to allow one prompt at global level to direct the agent to find functions. Function descriptions or hints in data are then designed to "re-prompt" the agent but these prompts should feel like natural documentation and not a black art. In this way the agent (or perhaps a small number of agents) are fixed while the design goal is to think about how to document functions for use in this ecosystem. More on functions later.

The global prompt on the base InterpreterAgent was set up to do a few things;

- firstly, to provide some basic Resonance context and guidance
- secondly. provide advice on how to use different stores (there is also a lookup for more strategic advice which loads one or more readmes)
- thirdly, to require the agent to always state and therefore reflect on its strategy - including details on function calling
- lastly, to report its confidence as a score from 0 to 100

Note: _This structured response data is always logged so we can later study how particular functions and data contribute to overall confidence in answers._

You can check out the prompt in the codebase to dig more. If you want, you can subclass the InterpreterAgent and change only the `PLAN` to create other behaviours, while making use of the rest of the function calling infra (if useful)

Note: _If you want to bypass ths prompt and the interpreter loop pattern and just use the open ai chat completion interface directly you can call the `ask` method instead. this is merely a convenient wrapper and nothing more. You can also `describe_visual_image`._

### Other boilerplate

- The `run` function does a bunch of extra stuff that has nothing to do with chat per se such as auditing or tracking some slack and user context.  The interpreter agent has some other functions such as `summarize` which exists to compress context or data if we are worried about context window lengths.

- Each session is logged using one of the vector stores as can be seen in the `_dump` method

- There is a crude method to scrape references to s3 files in certain responses so we can send attachments to slack. For example there is an API call to get a PNG of a nest which can be sent to a slack thread.

# 2 Functions

Functions play a huge role in the ASK.ONE setup. There are some utilities to take any (properly documented) function and wrap it in a function descriptor type. Form this we can describe the function in the format that OpenAI requires for example. For any function we have loaded, we can use its `function_description()` to generate a dictionary for use in the call to the open ai chat completion endpoint.

This idea can be used widely because we can take special stores like the entity store, vector stores and columnar stores and  wrap their `run_search` method for use in the interpreter agent loop thus giving access to our data. Similarly we can index APIs from their OpenAPI.json using utilities in the `dataops` module. These stores are described in more detail below.

## 2.1 The function interface

Its very important to properly type and document functions for use in this framework. If you dont, the current version may give cryptic errors. Here is an example of a properly documented function. It is the built-in function that the `InterpreterAgent` calls to revise functions;

```python

    def revise_functions(
        cls,
        questions: typing.List[str],
        issues: str = None,
        prune: bool = False,
        limit: int = 3,
        function_class: str = None,
    ):
        """This function can be used to search for other functions and update your function list. Call this method if you are struggling to answer the question by splitting your needs into separate questions.
            Guidelines:
            - if given specific entities or you want to know about entity relationships, just call the entity store and not this
            - api search is useful for very specific functions when the parameters e.g. identifiers are known
            - use columnar stores for most stats, facts, comments etc.
            - use vector store for adding color or for general vague or open-ended user questions
        **Args**
            questions: provide a list if precise questions to answer. Splitting up questions is useful.
            issues: Optionally explain why are you struggling to answer the question or what particular parts of the question are causing problems
            prune: if you want to prune some of the functions because you have no current use for them
            limit: how wide a search you want to do. You can subselect from many functions or request a small number. use the default unless you are sure you know what you want. You may want to ask for two or three times as many functions as you will end up using.
            function_class: the class of function to search for - Any|VectorSearch|ColumnarQuantitativeSearch|api
        """
```

Observe the following from that signature and doc string;

- the docstring uses the Google format for args
- The function description is a prompt to the LLM - when it searches all functions in the "function stack", this is how it knows what the function can do and how to call it
- The args are typed with full python typing information which is needed to build the function description that we send to OpenAI

Now if we import the function description we can describe this function

```python
from res.observability.dataops import (
    describe_function,
    FunctionDescription,
)

fd: FunctionDescription = describe_function(cls.revise_function)
what_we_send_to_openai = fd.function_dict()
```

## 2.2 Searching functions

We register functions such as API endpoints or the data store searches as functions. These are stored in a searchable vector store. By storing functions (and their descriptions) in a vector store, we can do a vector search on functions whose description can match the question context. We can then load and search matching functions. As mentioned above this is done serially today but in future will be parallel search.

## 2.3 Functions and stores

There are a few different store types that have pros and cons

### 2.3.1 Entity Store

The entity store is a Redis wrapper which takes keys like body codes, skus, one numbers etc and return values. This is simple and powerful because its easy to populate with useful data, hints for the agent etc and the interface is easy i.e. if you have one or more keys, you can fetch your data unlike the more error prone construction of the right SQL statement or framing the right question for vector search. Today each queue (Body, Style, Production Requests, Customer orders) keep all keys up to date for stuff moving through the queue. We run this update on a cron job every hour or so. One way to use an entity store is to get a handle of the one the InterpreterAgent uses. (You can also easily new it up from anywhere)

Firstly, you should open production Redis port with kube forwarder. Then

```python
agent = InterpreterAgent()
es = agent._entity_store

es['KT-2011']

```

### 2.3.2 Vector Data Store

The `VectorDataStore` uses LanceDB to store arbitrary text as embeddings. Generally our stores use S3 as storage and we generally use a namespace and name to describe the store location. ASK.ONE does not use one centralized vector store but uses an arbitrary number of separate stores in this way. The `run_search` method can be tested directly to test vector search. If you want to create a new store all you need to is use or sub class the `AbstractVectorStore` object. Here is an example

```python
from res.observability.entity import AbstractVectorStoreEntry
from res.observability.io import VectorDataStore 
key ='whatever'
text='stuff'

key = key or res.utils.res_hash()

#the model implicitly knows to use the open ai embedding and here we describe the location
Model = AbstractVectorStoreEntry.create_model(
    name="testing", namespace="default"
)

#create a record as an instance of the model
data = Model(
    name=key,
    text=text,
    doc_id=doc_id or key,
    timestamp=res.utils.dates.utc_now_iso_string(),
)

#the store is controlled by the model i.e. where stuff gets stored
my_store = VectorDataStore(Model)

store.add(data)
#this calls unto run_search
store('ask a question')
```

To find other stores you can can get a handle to the agent's function store and load them all

```python
data = agent._function_index_store.load()[
                ["name", "text", "function_namespace", "function_class", "id"]
            ]
```

For example you will find some vector stores in the `slack` namespace because we index certain slack channels every day. The slack channel `#artifical-news` summarizes and reports on these at about 19:30 ET every day. We also index some stuff in coda and from other places.

### 2.3.2 Columnar Data Store

The `ColumnarDataStore` follows all the same principles so you can import with,

```python
from res.observability.io import ColumnarDataStore
```

and do most of the stuff above. However, columnar stores normally have some specific schema of interest. There are examples in the codebase of using this, which you can check, but you basically just want to subclass the `AbstractEntity` object, add your fields and do as above.

The only other interesting thing about the `ColumnarDataStore` is that unlike the `VectorDataStore` that does a dumb vector search, the columnar store actually takes the english question, makes a call out to openAI to get an SQL query and _then_ runs the query against the store and returns the data.

The ColumnarDataStore is itself a useful sub module. It takes an english question in context of the columns and enums in its data store and asks GPT to convert the question to an SQL statement that is executed against the DuckDB Store and the records returned.

There is plenty of opportunity to isolate and test this to get the most out of tabular data sets. This can be a very effective and direct source. There are some lessons learned already in the Columnar Data Store such as

- the value of seeding with enums e.g. distinct lists of specific columns - this is useful e.g. to be invariant to specific spellings or to guide the model in other ways
- being very particular about names and types of columns to not mislead the model. For example if something is an ordinal or boolean etc it should be named as such. Also, if something is a date, it is better to save it as a date instead of a string that needs to be case which can lead to unwieldy generated SQL

## Using the store as an agent

By calling `my_store.as_agent()` we can get a reference to an agent that is primed to use the stores search function and not try to revise functions. This is a way to test stores in isolation and usefulness in answering targeted questions.  

# Slack Integrations

The Ask.One bot is a slack app that uses the Interpreter Agent to answer user queries. We response to the same channel with a response. A few comments are worth making here

- we send the slack user and channel into the agent run method - among other things we can keep track of the context e.g. to allow for some sort of conversational continuity. A very crude approach is used to keep a rolling window of recent conversation in a cache.
- We response to the user in the same channel but not as a thread. We could, in some or all cases, reply in thread instead - this just requires passing the thread `ts` value in the reply but we have not done that so to date
- We allow for attachments to be returned by moving data to S3 and uploading to slack channels e.g. image attachments
- In some cases we change the mode of the agent in specific channels. For example we can bootstrap the agent with a restricted set of functions and disable function revision. In future we might use different agents (alternate PLANs)

# Research directions

1. The Columnar Data Store Text-to-SQL smarts: Because the columnar data store has an internal call to an LLM, there is opportunity to fine-tune it in isolation. Given some Pydantic object you control which is saved as parquet i.e. the underlying store. You can then improve the english-to-SQL prompting etc. to get the most out of structured data stores.
2. Subclass the InterpeterAgent and try different PLANs or alternate logic: This is an easy way to try to improve the agent. Keep in mind that the current prompt has been refined and regression tested against lots of different types of questions and its difficult to know what works well generally. While its possible to improve this agent overall, its also interesting to consider more specialist agents that work very well in more narrow contexts. In the limit this is necessary yet its useful to get every bit of juice out of a single agent.
3. The guts of the interpreter loop and how it calls functions e.g. in parallel can be improved: One possible next step is to abstract out parallel store-calling and spin up a dedicated horizontally scalable microservice. One way to do this might be to use a Ray Cluster which is one of the better ways to do parallel python. This provides mostly speed benefits but there are also opportunities to get better search results via re-ranking and assimilating results across many stores.
4. Different agent architectures such as the Planner Agent: I started in the code base. The idea is to build a structured DAG of nested function  calls. Functions can be called in parallel in waves with context  passed on to the next wave of function calls. Workers are then used to execute the full predetermined plan. This is an interesting problem because questions of how/when/if workers can adapt or change plans and how to pass context overall are difficult to answer in general, or so i have found from testing in different scenarios. The opportunity (why it might be worth the effort) is that pre-planning followed by execution could lead to more powerful ways to decompose very complex plans and also understand and debug the execution of different stages.
5. There are more specialist tasks such as document parsing which im starting to extract knowledge from tech packs. This work requires using different modalities and also building workflows to "work through" a document to extract insights.
6. etc.
