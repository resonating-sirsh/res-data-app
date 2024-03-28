"""
certain workflow logic wrapping DB logic
"""


"""
temporary hold for queue definitions
a queue is defined by name, key, predicates - we can construct a mutation to select pydantic objects filtered by this
so really most of the definitions are metadata associated with the type system
a queue could be just a mutation with a data window and a boolean switch 
we need a way to flatten it to a pydantic object which is the thing that literally goes to be updated in airtable

A Q is a + and - part. Stuff arriving to and leaving the queue
the queue is defined by a boolean and a date range. 
- the boolean switch sets up the predicate to show things in queue and out of queue
- the data range then captures changes to the queue + and - within some time window
this could be used to determine things to purge 
- when we purge we should remove airtable keys from cache and record it in the queue
 - this is so if the message somehow re-appears we are not expecting it in airtable
 - in this way the hasura queue provides the full list of things that are in airtables -> add airtable metadata 
"""

from schemas.pydantic.make import OneOrderResponse
from res.flows.api import FlowAPI
import res


def process_evictions(event, context={}):
    """
    dliddane 2-Nov-23 - it doenst look like this is being used yet, but to be sure I'll wait till sirsh returns - in the meantime I will duplicate and create a sql version
    Simply determine all registered queue types and call their evict on the date range
    we add this daily on the flow scheduler
    """

    # hard coding the list is fine for now
    types = []
    window_from = event.args.get("window_from", res.utils.dates.relative_to_now(1))
    window_to = event.args.get("window_to", res.utils.dates.utc_now())

    for t in types:
        FlowAPI(t).process_queue_evictions(window_from=window_from, window_to=window_to)

    return {}


def handler(event, context={}):
    """this might be needed for argo to see the function?"""
    process_evictions_sql(event, context)


def process_evictions_sql(event, context={}):
    """

    Simply determine all registered queue types and call their evict on the date range
    we add this daily on the flow scheduler
    """

    # hard coding the list is fine for now .. nudge
    types = [OneOrderResponse]

    dict_relative_dates = {
        OneOrderResponse: (103, 100)
    }  # the values of the tuple are = days back in history to run query from, and to
    for t in types:
        days_from = res.utils.dates.relative_to_now(
            dict_relative_dates[OneOrderResponse][0]
        )

        days_to = res.utils.dates.relative_to_now(
            dict_relative_dates[OneOrderResponse][1]
        )

        window_from = res.utils.dates.coerce_to_full_datetime(
            event.get("args", days_from).get("window_from", days_from)
        )

        window_to = res.utils.dates.coerce_to_full_datetime(
            event.get("args", days_to).get("window_to", days_to)
        )

        FlowAPI(t).process_queue_evictions_sql(
            window_from=window_from, window_to=window_to
        )

    return {}
