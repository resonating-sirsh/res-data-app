import os

# if "HASURA_ENDPOINT" in os.environ:  # nudge
#     os.environ.pop("HASURA_ENDPOINT")
# use_env = "production"
# os.environ["RES_ENV"] = "production"
# os.environ["HASURA_ENDPOINT"] = "https://hasura.resmagic.io"
# os.environ["KAFKA_KGATEWAY_URL"] = "https://data.resmagic.io/kgateway/submitevent"
# rds_server = "localhost:5432"
rds_server = None
import traceback

import res


import json
import pandas as pd
from psycopg2.extras import execute_values

# import res.flows.make.production


from schemas.pydantic.make import *

from datetime import datetime, timedelta, timezone

from warnings import filterwarnings


def get_dataframe(conn, sql_query):
    # Execute the SQL query select  * from infraestructure.dead_letters

    res.utils.logger.info("Connected to postgres. Getting postgres cursor...")
    cursor = conn.cursor()
    res.utils.logger.info(f"Got cursor, running query {sql_query}...")
    cursor.execute(sql_query)

    res.utils.logger.info(f"Query has been ran successfully")
    query_results = cursor.fetchall()

    # Get the column names from the cursor description
    column_names = [desc[0] for desc in cursor.description]

    # Close the cursor and the database connection

    df = pd.DataFrame(query_results, columns=column_names)
    return df


def handler(event={}, context={}):
    """
    for now running on a schedule
    """
    try:
        filterwarnings("ignore")

        messagesToPublish = []
        with res.flows.FlowContext(event, context) as fc:
            res.utils.logger.info(f"getting connection, rds_server: {rds_server} ")
            conn = res.connectors.load("postgres", rds_server=rds_server)
            res.utils.logger.info(f"Got Connection")

            if event.get("kafka_configs"):
                kafka_configs = event.get("kafka_configs")
            else:
                kafka_configs = [
                    {
                        "topic": "res_make.orders.one_requests",
                        "minimum_delay_since_created_mins": 5,
                    }
                ]

            topics = [d["topic"] for d in kafka_configs]

            # Write your SQL query as a string
            sql_query = f"""select  * from infraestructure.dead_letters
                    where name in ('{''','''.join(topics)}')
                    and reprocessed_at IS NULL """
            res.utils.logger.info(f"running query: {sql_query}")

            df = conn.run_query(sql_query, keep_conn_open=True)

            res.utils.logger.info(f"rows retreived From DB: {df.shape[0]}")

            s3 = res.connectors.load("s3")
            kafka = res.connectors.load("kafka")

            now = pd.Timestamp.utcnow()
            for topic in topics:
                msgsForTopic = df[df["name"] == topic]
                msgsForTopic.sort_values("updated_at", ascending=True)
                msgsForTopic["republished"] = False
                matching_config = next(
                    (config for config in kafka_configs if config["topic"] == topic),
                    None,
                )
                updated_before = now - timedelta(
                    minutes=matching_config["minimum_delay_since_created_mins"]
                )

                msgsForTopic = msgsForTopic[msgsForTopic["updated_at"] < updated_before]
                res.utils.logger.info(
                    f"rTopic: {topic} - rows left after filtering After filtering for updated_at < {updated_before}  rows left: {msgsForTopic.shape[0]}"
                )
                messagesToPublishForTopic = []
                for iter, row in msgsForTopic.iterrows():
                    res.utils.logger.info(f"Loading Json from S3: {row['uri']}")
                    s3json = s3.read(row["uri"])

                    messagesToPublishForTopic.append(s3json)

                    msgsForTopic.at[iter, "republished"] = True

                messagesToPublish.append(
                    {"topic": topic, "messages": messagesToPublishForTopic}
                )

                reprocesseddf = msgsForTopic[msgsForTopic["republished"] == True]
                # the updated_before before check is to confirm that the kaf message we sent has not already been processed and updated the dead_letter table again

                update_reprocess_attempt(conn, reprocesseddf, updated_before, now)

                conn.close()

                for dictListMessages in messagesToPublish:
                    try:
                        for message in dictListMessages["messages"]:
                            res.utils.logger.info(
                                f"About to publish for {dictListMessages['topic']} message:  {json.dumps(message, indent=4)}"
                            )
                            kafka[dictListMessages["topic"]].publish(
                                message, use_kgateway=True
                            )
                    except Exception as ex:
                        res.utils.logger.warn(
                            f"Failed to publish kaf message for {topic} ex: {traceback.format_exc()}"
                        )
    except Exception as e:
        # show full stack trace

        res.utils.logger.warn(
            f"\n\n\n\n\n**ERROR**: Top level Exception at the level of the handler: {traceback.format_exc()}"
        )
        return {}


def update_reprocess_attempt(conn, df_reprocessed, update_before, now):
    with conn.cursor() as cursor:
        create_table_sql = """CREATE TEMPORARY TABLE id_values (
            id UUID,
            counter INTEGER
        )
        """
        cursor.execute(create_table_sql)
        df_reprocessed["counter"] = df_reprocessed["counter"].apply(
            lambda x: x if pd.notna(x) else pd.NA
        )
        df_reprocessed["counter"] = df_reprocessed["counter"].fillna(0)
        df_reprocessed["counter"] = df_reprocessed["counter"].apply(lambda x: x + 1)
        toupdate_df = df_reprocessed[["id", "counter"]]
        toupdate_tp = list(toupdate_df.itertuples(index=False, name=None))
        # Insert data into the temporary table using execute_values
        execute_values(
            cursor, "INSERT INTO id_values (id, counter) VALUES %s", toupdate_tp
        )

        res.utils.logger.info(f"df_reprocessed shape: {df_reprocessed.shape}")
        res.utils.logger.info(f"df_reprocessed head (5): {df_reprocessed.head(5)}")
        res.utils.logger.info(
            f"updated rows into temp table id_values, rows affected: {cursor.rowcount}"
        )

        updateQuery = f"""
            UPDATE infraestructure.dead_letters AS dl
            SET reprocessed_at = '{now}',
                counter = iv.counter
            FROM id_values iv
            WHERE dl.id = iv.id
            and  updated_at < '{update_before}'
            """

        res.utils.logger.info(f"running: {updateQuery}")
        conn.run_update(updateQuery, keep_conn_open=False)

        res.utils.logger.info(f"updated rows with {now} as republished date")

        # Remember to commit the changes and close the connection


if __name__ == "__main__":
    # if calling this locally you can supply your own config as as event as follows:
    # handler(
    #     event={
    #         "kafka_configs": [
    #             {
    #                 "topic": "res_make.orders.one_requests",
    #                 "minimum_delay_since_created_mins": 3,
    #             }
    #         ]
    #     }
    # )
    handler()
