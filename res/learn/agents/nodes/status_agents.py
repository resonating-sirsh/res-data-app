import res
import traceback
from langchain.agents import Tool
from res.learn.agents.data.EntityDataStore import EntityDataStore
from res.learn.agents.data.ColumnarDataStore import (
    stats_tool_using_text_to_sql_for_df,
    ColumnarDataStore,
)
from res.learn.agents.data.VectorDataStore import get_text_tool
from res.learn.agents.NodeAgent import make_node_agent_from_tools


def get_agent(namespace, entity_name, try_and_add_entity_resolver_fn=None):
    """
    half baked pattern to pass some context functions to tools
    try_and_add_fn -> at the moment we can use keys and map them e.g. a four sku could be resolved as a three part and and added size
                      and other example, is order numbers with 4 digits can collide with body numbers so we add a hash for now
                      both of these things could be solved by using separate entity stores but its convenient not to do that for now
    """
    text_tool = None
    try:
        text_tool = get_text_tool(namespace=namespace, entity_name=entity_name)
    except:
        res.utils.logger.warn(
            f"Failed to load the text tool - permissions issue perhaps"
        )
        res.utils.logger.warn(traceback.format_exc())

    er_tool = EntityDataStore(
        entity_name=entity_name,
        namespace=namespace,
        try_and_add_fn=try_and_add_entity_resolver_fn,
    ).as_tool()
    stats_tools = ColumnarDataStore(
        entity_name=entity_name, namespace=namespace
    ).as_tool()

    try:
        tools = (
            [er_tool, stats_tools, text_tool] if text_tool else [er_tool, stats_tools]
        )

        return make_node_agent_from_tools(tools)

    except:
        res.utils.logger.warn(f"Failing to load the agent")
        res.utils.logger.warn(traceback.format_exc())


def get_style_agent():
    """

    agent_executor.run(f"What is wrong with style CC-9003 FBTRY BOUNAW")
    agent_executor.run("How many Custom styles are in the status place color elements?")
    agent_executor.run("What are the contract variables for styles that are not done and assigned to ekim?")
    agent_executor("What is the body belonging to KT-3030 CTW70 DMTEPT.  Does frank like the body?")
    agent_executor.run("What is the body code and available sizes for style KT-3030 CTW70 DMTEPT and how many sizes are there? What are the color and material code of this style?")
    agent_executor.run("Will we need to repair garment TK-3075 CTW70 DMPLED")

    #
    List three example orders (with order numbers and skus) for orders by JCRT brand this week?
    How many orders were there for the KIT brand this week
    """
    return get_agent("meta", "style")


def get_body_agent():
    return get_agent("meta", "body")


def get_orders_agent():
    """
    a stop gap solution for passing key context
    orders are going to be stored as #AA-12345 so as not to collide with body codes until we find another partitioning scheme
    """

    return get_agent(
        "sell", "orders", try_and_add_entity_resolver_fn=lambda k: (f"#{k}", None)
    )


def get_one_agent():
    return get_agent("make", "one")


def get_super_agent(model="gpt-4", temperature=0.09):
    """
    A super tool for asking more ONE level questions
    """
    res.utils.logger.info(
        "Loading agents - may take a moment as we inspect some data..."
    )
    body_agent = get_body_agent()
    style_agent = get_style_agent()
    orders_agent = get_orders_agent()
    one_agent = get_one_agent()

    tools = [
        Tool(
            name=f"If you have questions about any bodies or the body request queue use this tool",
            func=body_agent.run,
            description=f"""This tool is a wrapper tool the ask questions about bodies""",
        ),
        Tool(
            name=f"If you have questions about any styles or the apply color use this tool",
            func=style_agent.run,
            description=f"""This tool is a wrapper tool the ask questions about styles and the apply color queue""",
        ),
        Tool(
            name=f"If you have questions about customer orders or product sales or fulfillment use this tool",
            func=orders_agent.run,
            description=f"""This tool is a wrapper tool the ask questions about customer orders or product sales status""",
        ),
        Tool(
            name=f"If you have questions about ONE orders or make production requests or indeed anything related to manufacturing processes like Print, Cut or Sew, then use this tool",
            func=one_agent.run,
            description=f"""This tool is a wrapper tool the ask questions about ONE orders or make production requests status""",
        ),
    ]

    return make_node_agent_from_tools(tools=tools, model=model, temperature=temperature)


# def get_body_agent(llm_model="text-davinci-003", refresh=False):
#     if refresh:
#         bq = airtable.get_view_data(
#             "appa7Sw0ML47cA8D1/tblrq1XfkbuElPh0l/viwjfzvLDWW6bqT6p",
#             fields=[
#                 "Name",
#                 "Body Number",
#                 "Modification Request Type (RUN v DEV)",
#                 "Date Created",
#                 "Status",
#                 "Body ONE Ready Request Type",
#                 "Flag for Review Reason",
#                 "Feedback Requests",
#                 "Internal Digital Engineering Notes",
#                 "Brand Success Notes",
#                 "Brand Success Flag",
#                 "Active Brand Moment Launch Dates",
#                 "Development Type",
#                 "Sew Development Review Notes",
#                 "Feedbacks Closed and ONE Ready",
#             ],
#         )  #'Flag for Review: Tag',
#         bq["Body Code"] = bq["Body"] = bq["Body Number"] = bq["Body Number"].map(
#             lambda x: x[0] if isinstance(x, list) else None
#         )
#         bq["Feedback Requests Link"] = bq["Feedback Requests"].map(
#             lambda x: f"https://airtable.com/appa7Sw0ML47cA8D1/tblORtfvTdeWZxPkR/{x[0].split('?')[0]}"
#             if isinstance(x, list)
#             else None
#         )
#         bq["Feedback Requests"] = bq["Feedback Requests Link"]
#         pd.set_option("display.max_colwidth", 80)

#         s3.write(
#             "s3://res-data-platform/queue-status-snapshots/bq.feather",
#             bq.reset_index().drop("index", 1),
#         )
#     bq = s3.read("s3://res-data-platform/queue-status-snapshots/bq.feather")
#     bq_agent = create_pandas_dataframe_agent(
#         OpenAI(temperature=0, model_name=llm_model), bq, verbose=True
#     )

#     return bq_agent
