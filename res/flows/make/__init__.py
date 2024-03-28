from .. import FlowContext


def relay_batch_events(*args, **kwargas):
    # for make events we are going to get some metadata
    """
    Factory order number/ ONe number
    Order Status (-1: cancelled 0 closed 1 open) this MIN aggregate can be used in druid
    Add maxaggregate to druid on date field in unix time or whatever works
    Add body, material, color, size, brand attributes
    All of these slowly changing attributes should be cached in a factory status KV such as dynamo later redis under presto
    Current node and flow could also be cached as we have to fetch when they change anyway
    """
    pass


# can we maybe handle some printer events too? For druid reporting


def process_events(message_batch):
    pass
