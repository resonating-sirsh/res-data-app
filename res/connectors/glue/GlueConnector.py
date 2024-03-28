import boto3
import os
import res


class GlueConnector(object):
    def __init__(self):
        self._athena_client = self.get_client()

    def get_client(self, **kwargs):
        return boto3.client(
            "glue",
            region_name="us-east-1",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        )

    def _create_datbase(self, name):
        """
        should not need to do this often
        """
        response = self.get_client().create_database(
            DatabaseInput={
                "Name": "flow_db",  # Required
                "Description": "Flow DB for nodes in the flow api to created logs and structured data dumps",
                "Parameters": {"test": "key"},
            }
        )

    def _norm_name(self, entity):
        # todo regex
        return entity.lower().replace("/", "_")

    def _entity_path(self, entity):
        return "s3://res-data-platform/entities/{entity}"

    @staticmethod
    def column_spec_from_df(df):
        """
        using python types map onto glue types
        for geometry columns we may want to do something like strings -> lets do strings for all objects for now
        binary formats also possible but not worth the hassle at our data scales (arguably)
        """
        return []

    def create_table_for_parquet_folder(
        self, name, path=None, database="flow_db", parameters={}, **kwargs
    ):
        """
        Simple way to register parquet files on the cluster
        in future we could support other things

        path could be a file but assuming a directory with files
        """
        s3 = res.connectors.load("s3")
        description = kwargs.get("description", "res-data generated table")
        hint_df = kwargs.get("hint_df")
        if hint_df is None:
            assert (
                path is not None
            ), "unless you specify a hint dataframe to infer types you must point to a location with existing files"
            # sample the folder
            f = None
            for f in s3.ls(path):
                break
            assert (
                f is not None
            ), "unless you specify a hint dataframe to infer types you must point to a location with existing files"

            hint_df = s3.read(f)

        spec = GlueConnector.column_spec_from_df(hint_df)

        return self._get_client().create_table(
            DatabaseName=database,
            TableInput={
                "Name": name,
                "Description": description,
                "StorageDescriptor": {
                    "Columns": spec,
                    "Location": "s3://res-data-platform/samples/glue-test/a",
                },
                "Parameters": parameters,
            },
        )

    def __create_table_sample(self, table):
        response = self._get_client().create_table(
            DatabaseName="flow_db",
            TableInput={
                "Name": table,
                "Description": "Table created with boto3 API",
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "key",
                            "Type": "string",
                            "Comment": "This is very useful column",
                        },
                        {
                            "Name": "uri",
                            "Type": "string",
                            "Comment": "This is not as useful",
                        },
                    ],
                    "Location": "s3://res-data-platform/samples/glue-test/a",
                },
                "Parameters": {
                    "classification": "parquet",
                    "typeOfData": "file",
                },
            },
        )
