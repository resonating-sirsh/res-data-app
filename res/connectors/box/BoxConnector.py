"""
Simple wrapper to load box client for our environment and download files
https://github.com/box/box-python-sdk/blob/main/docs/usage/files.md

"""

import io
import os
import tempfile
import boxsdk
from boxsdk import JWTAuth, Client
import boto3
from res.utils import secrets_client, logger
import pandas as pd
import json
import os

NET_FILE_FOLDER_ROOT = "41568396914"


class BoxConnector:
    def __init__(self):

        # Retrieve secret and create a config file
        config_json = secrets_client.get_secret("BOX_DATA_INTEGRATION_CONFIG_JSON")

        with open("config.json", "w") as json_file:

            json.dump(config_json, json_file)

        # Read JSON configuration and initialize SDK client
        config = JWTAuth.from_settings_file("config.json")
        self.client = Client(config)
        self.user = self.client.user().get()
        os.remove("config.json")

    def _rsa_from_param_store(self):
        parameter = self.client.get_parameter(
            Name="/resmagic/api/prod/box/rsa_private_key", WithDecryption=True
        )
        return parameter["Parameter"]["Value"]

    def _get_net_files(self, brands=None, bodies=None):
        """
        attempt to read through NET files based on conventions of how they are saved
        not sure how trustworthy this is but we can qa the net file list for all brands
        if we fix each brands location it should not change and then we can get the list of net files for their bodies
        from there we sync modified files to s3

        here is an example, given a list of files we can move the box files if newer to s3

        from tqdm import tqdm
        for row  in tqdm(data.to_dict('records')):
            #check timestamp on s3 file to see if its newer and only copy newer
            box.copy_to_s3(row['dxf_file_id'], row['s3_name'])

        """

        def s3_file_name(f):
            """
            s3://meta-one-assets-prod/bodies/net_files/[body_code]/v[body_version]/[filename].dxf
            """
            name = f.lower().replace("-", "_")

            body_code = "-".join(f.split("-")[:2]).lower().replace("-", "_")
            version = f.split("-")[2].lower()

            path = f"s3://meta-one-assets-prod/bodies/net_files/{body_code}/{version}/{name}"

            return path

        c = self.client
        folder = c.folder(folder_id=NET_FILE_FOLDER_ROOT).get()
        # brands = ['TUCKER']
        # bodies = ['TK-6136']

        data = []
        for i in folder.get_items(limit=10):
            if i.name in brands or not brands:
                # filter for brand here
                if i.object_type == "folder":
                    for f in i.get_items():
                        if (
                            f.object_type == "folder"
                            and "aimarkers" in f.name.lower().replace(" ", "")
                        ):
                            for setup in f.get_items():

                                if "setup" in setup.name.lower().replace(" ", ""):
                                    for body in setup.get_items():
                                        body_part = "-".join(body.name.split("-")[:2])

                                        if body_part in bodies or not bodies:
                                            # filter for body here

                                            if body.object_type == "folder":
                                                for find_dxf in body.get_items():
                                                    if ".dxf" in find_dxf.name.lower():
                                                        d = {
                                                            "brand": i.name,
                                                            "brand_folder_id": i.id,
                                                            "setup_folder_id": setup.id,
                                                            "body": body.name,
                                                            "dxf_file_name": find_dxf.name,
                                                            "dxf_file_id": find_dxf.id,
                                                            # todo add mod date
                                                        }
                                                        data.append(d)

        data = pd.DataFrame(data)
        # for convenience we preempty where we would store it on s3
        data["s3_name"] = data["dxf_file_name"].map(s3_file_name)
        return data

    def download_file(self, file_id, path):

        with open(path, "wb") as f:
            self.client.file(file_id).download_to(f)

    def copy_to_s3(self, file_id, path):
        from res.connectors.s3 import S3Connector

        s3 = S3Connector()
        with s3.file_object(path, mode="wb") as f:
            self.client.file(file_id).download_to(f)

    def upload_file(self, folder, name, filepath):
        root_folder = self.client.folder(folder_id=folder)
        file_path = filepath
        try:
            a_file = root_folder.upload(file_path, file_name=name)
            logger.info("File uploaded to Box!")
        except boxsdk.exception.BoxAPIException as e:
            if "Item with the same name already exists" in str(e):
                logger.warn("File already exists! Ignoring upload.")
            else:
                raise

    def upload_from_stream(self, folder_id: str, stream, file_name: str):

        new_file = self.client.folder(folder_id).upload_stream(stream, file_name)

        return new_file

    def upload_new_version_from_stream(self, file_id: str, stream):

        new_file = self.client.file(file_id).update_contents_with_stream(stream)

        return new_file

    def get_folder_items(self, folder_id):

        items = self.client.folder(folder_id=str(folder_id)).get_items()
        item_list = []

        for item in items:

            item_list.append(item)

        return item_list

    def create_subfolder(self, parent_folder_id, folder_name):

        subfolder = self.client.folder(parent_folder_id).create_subfolder(folder_name)

        return subfolder
