from res.connectors import load
from res.connectors.mongo.MongoConnector import MongoConnector


def get_mongo_connector():
    mogon_conn: MongoConnector = load("mongo")
    db = mogon_conn.get_client().get_database("resmagic")
    try:
        yield db
    finally:
        db.client.close()
