import os
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import dns  # <-this does not need to be imported but added so pipreqs picks up the dep
import pandas as pd
from bson.objectid import ObjectId
from pymongo import MongoClient

import res
from . import DatabaseConnector
from res.connectors import load


class MongoConnector(DatabaseConnector):
    def get_client(self, **kwargs) -> MongoClient:

        if not "MONGODB_USER" in os.environ or not "MONGODB_PASSWORD":
            raise Exception("MongoDB credentials missing")

        host = os.getenv("MONGODB_HOST", "resmagic.fahv4.mongodb.net")
        user = quote_plus(os.getenv("MONGODB_USER", ""))
        password = quote_plus(os.getenv("MONGODB_PASSWORD", ""))

        # i think we could add kwargs for an optional database arg
        return MongoClient(
            "mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority"
            % (user, password, host)
        )

    def __getitem__(self, key):
        c = self.get_client()
        return c[key]

    @staticmethod
    def key_lookup(collection, key):
        mongo = load("mongo")
        db = mongo["resmagic"]
        return pd.DataFrame(
            db.get_collection(collection).find({"_id": MongoConnector.object_id(key)})
        )

    @staticmethod
    def object_id(s):
        return ObjectId(s)

    def legacy_add_resmagic_file(self, s3_path, size, file_type, created_by=None):
        """
        We can add files to our Mongo collection
        Files are a lookup of S3 objects
        In the data lake mode we happily store S3 files with semantics but in existing systems we may need to register the file

        path: full s3 path e.g. s3://bucket/path/to/stem.ext
        file_type: examples image/ong, application/pdf etc.

        The upsert is key'd on the filename and bucket so we only keep on file record per file - see `predicate` below

        """
        s3 = load("s3")
        files_collection = self.get_client()["resmagic"]["files"]
        bucket, filename = s3.split_bucket_and_blob_from_path(s3_path)
        name, ext = Path(s3_path).name, Path(s3_path).suffix

        input_body = {
            "name": name,
            "extension": ext,
            "type": file_type,
            "size": size,
            "bucket": bucket,
            "s3": {"bucket": bucket, "key": filename, "id": filename},
            "storageType": "s3",
            "updateAt": datetime.utcnow(),
            "createdBy": created_by or "res-data",
        }

        # search for this unique s3 object
        predicate = {"s3.key": filename, "s3.bucket": bucket}

        _existing = files_collection.find_one(predicate)

        if _existing:
            res.utils.logger.debug("Founding existing to update")
            _existing.update(input_body)
            result = files_collection.update_one(predicate, {"$set": _existing})
            # return the id
            return str(_existing["_id"])

        else:
            res.utils.logger.debug("Adding a new record")
            result = files_collection.insert_one(input_body)
            return str(result.inserted_id)
