import duckdb
from res.utils import secrets_client
import res
import os


def escape(s):
    return s if not isinstance(s, str) else f"'{s}'"


class _query:
    """
    ***
    simple fluent query helper
    ***
    
    examples

    ```python
    #example that plans the query but does not run it
    duck.query_from_root('s3://meta-one-assets-prod/versioned-skus/api-v1/metadata/kt_2043').\
    where(sku='KT-2043 CTSPS BERRMZ ZZZ14', piece_type='block_fuse').\
    select(plan=True)

    #example with isin predicates that does run the query
    duck.query_from_root('s3://meta-one-assets-prod/versioned-skus/api-v1/metadata/kt_2043').\
    isin(sku=['KT-2043 CTSPS BERRMZ ZZZ14', 'KT-2043 CTSPS VIRIKE ZZZ06'], piece_type='block_fuse').\
    select(plan=False)

    #example with select fields
    duck.query_from_root('s3://meta-one-assets-prod/versioned-skus/api-v1/metadata/kt_2043').\
    isin(sku=['KT-2043 CTSPS BERRMZ ZZZ14', 'KT-2043 CTSPS VIRIKE ZZZ06'], piece_type='block_fuse').\
    select(['key', 'sku', 'piece_type', 'annotated_image_uri'])

    ```
    """

    def __init__(self, engine, root):
        self._table = root
        self._predicates = []
        self._engine = engine

    def where(self, **kwargs):
        """
        only support and predicates right now
        chained filters that can then be sub filtered in pandas
        """
        for k, v in kwargs.items():
            self._predicates.append(f"{k}={escape(v)}")
        return self

    def isin(self, **kwargs):
        """
        only support and predicates right now
        chained filters that can then be sub filtered in pandas
        """
        for k, values in kwargs.items():
            if not isinstance(values, list):
                values = [values]
            values = f",".join([escape(s) for s in values])
            self._predicates.append(f"{k} in ({values})")
        return self

    def select(self, fields=None, plan=False):
        fields = "*" if fields is None else ",".join(fields)
        query = f"""SELECT {fields} from read_parquet('{self._table}') """

        if self._predicates:
            query += "WHERE "
            query += " AND ".join(self._predicates)

        if plan:
            return query

        return self._engine.execute(query)

    def __repr__(self) -> str:
        return self.select(plan=True)


class DuckDBClient:
    def __init__(self, **options):
        self._cursor = duckdb.connect(**options)

        if "AWS_ACCESS_KEY_ID" not in os.environ:
            aws_keys = secrets_client.get_secret("AWSKEY_DEFAULT")
            os.environ["AWS_ACCESS_KEY_ID"] = aws_keys["ACCESS_KEY"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = aws_keys["SECRET_ACCESS_KEY"]

        AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            creds = f"""
                SET s3_region='us-east-1';
                SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';
                SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';"""

        self._cursor.execute(
            f"""
            INSTALL httpfs;
            LOAD httpfs;
            {creds}
        """
        )

    def probe_field_names(self, uri):
        if not res.connectors.load("s3").exists(uri):
            return []

        return list(self.execute(f"SELECT * FROM '{uri}' LIMIT 1").columns)

    def inspect_enums(
        self, uri, enum_threshold=50, max_str_length=100, omit_fields=None
    ):
        """
        inspect enums is used to send context to LLM
        dont use this if you have sensitive data in fields or add protection

        for example this can be used if we ask vague questions
        or questions that reference misspelled or alternately spelt data - the LLM can make sense of it

        this is probably necessary for SQL types to be useful but avoides sending too much data in context
        """

        if not res.connectors.load("s3").exists(uri):
            return {}
        df = self.execute(f"SELECT * FROM '{uri}'")

        def try_unique(c):
            try:
                # dont allow big strings (polars notation)
                # l = df[c].str.lengths().mean()
                l = df[c].map(lambda x: len(str(x))).mean()
                # filter by sending back max threshold in these cases
                if l > max_str_length or c in (omit_fields or []):
                    return enum_threshold
                # if we are happy, return the list of enumerated values for LLM context
                return len(df[c].unique())

            except Exception as ex:
                # print("Failed", ex)
                return enum_threshold

        columns = df.columns
        enum_types = [c for c in columns if try_unique(c) < enum_threshold]
        return {c: list(df[c].unique()) for c in df.columns if c in enum_types}

    def execute(self, query):
        """
        e.g
          c.execute(" SELECT body_code, body_version FROM read_parquet('s3://res-data-platform/samples/duck_test.parquet') where body_version == 2;")
        """
        return self._cursor.execute(query).fetchdf()

    def query_from_root(self, root):
        root = root.rstrip("/")
        if root[-1 * len(".parquet") :] != ".parquet":
            root += "/*.parquet"
        return _query(self, root)
