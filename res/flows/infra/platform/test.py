import res
import res
import time
import traceback
import pandas as pd


def get_missing_sew_resources():
    q = """query MyQuery {
    meta_body_pieces(distinct_on: sew_identity_symbol) {
        sew_identity_symbol
    }
    }"""

    try:
        data = pd.DataFrame(
            res.connectors.load("hasura").execute_with_kwargs(q)["meta_body_pieces"]
        )
        from res.utils.resources import read

        missing = []
        for k in data["sew_identity_symbol"]:
            if k is not None and str(k) != "None":
                try:
                    read(f"sew_identity_symbols/{k}.svg")
                except:
                    missing.append(k)
        return missing
    except Exception as ex:
        res.utils.logger.warn(f"Failing to load resources")
        res.utils.logger.warn(traceback.format_exc())
        return ["ALL - FATAL ERROR"]


def handler(event, context={}):
    #####
    ###  test loading resources on the server
    #####
    res.utils.logger.info("testing loading resources")
    m = get_missing_sew_resources()
    if len(m):
        res.utils.logger.warn(f"Missing {m}")
    else:
        res.utils.logger.info("all good")

    res.utils.logger.info("testing kafka libraries")

    #####
    ###  test some client libraries on the server
    #####
    try:
        kafka = res.connectors.load("kafka")[
            "res_examples.example_kafka_producer.basic_test_topic_2"
        ]
        kafka.publish(
            {
                "name": "sirsh",
                "age": 42,
                "favorite_color": "black",
                "ice_cream_flavor": "hazelnut",
            }
        )
        res.utils.logger.info("sleeping then consuming")
        time.sleep(2)
        message = kafka.consume()
        if len(message):
            res.utils.logger.info(dict(message.iloc[0]))

    except:
        res.utils.logger.warn(f"Failed kafka client lib test {traceback.format_exc()}")

    #####
    ### test nodes
    ### ppp m1 save images
    ### etc.
    #####
