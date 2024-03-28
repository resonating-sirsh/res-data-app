from res.connectors import (
    DatabaseConnector,
    DatabaseConnectorTable,
    DatabaseConnectorSchema,
)
from . import QuickbooksClient
import pandas as pd
import numpy as np
import boto3

ENDPOINTS = [
    "Account",
    "Bill",
    "BillPayment",
    "Deposit",
    "JournalEntry",
    "Payment",
    "PaymentMethod",
    "Purchase",
    "Vendor",
    "VendorCredit",
    "Invoice",
]

REPORTS = ["reports/TransactionList", "reports/JournalReport" "reports/GeneralLedger"]

PAGE_SIZE = 100


class QuickbooksConnector(DatabaseConnector):
    def __init__(self):
        pass

    def __getitem__(self, account):
        return QuickbooksConnectorSchema(account)

    @staticmethod
    def explode_line_items(
        df, entity, with_columns=["Id", "DocNumber", "TxnDate", "__created_timestamp"]
    ):
        """
        Quickbooks data has some conventional LineItem structure and we can explode child items
        We add our own control fields into the list that are migrated to the child e.g. timestamps
        """
        header = df[[c for c in with_columns if c in df.columns]]
        child_items = pd.DataFrame([d for d in df[["Line"]].explode("Line")["Line"]])
        return header.join(child_items, lsuffix=entity)


class QuickbooksConnectorSchema(DatabaseConnectorSchema):
    def __init__(self, account=None):
        self._account = account
        self.boto = boto3.client()

    def __getitem__(self, endpoint):
        return QuickbooksConnectorTable(self._account, endpoint)


class QuickbooksConnectorTable(DatabaseConnectorTable):
    def __init__(self, account_name, endpoint):
        self._endpoint = endpoint
        self._client = QuickbooksClient(account_name)

    def get_record_count(self):
        """
        Get the total record count. This is needed to determine how many pages when fetching all data
        """
        query = f"SELECT count(*) FROM {self._endpoint}"
        return self._client.query(query).json()["QueryResponse"].get("totalCount", 0)

    def query_end_point(self, start_position=0):
        """
        Helper method to get the page of data as a dataframe
        The connector does not support iterating directly over records which is a minor parasing overhead for the sake of simplicity
        """
        query = f"SELECT * FROM {self._endpoint} STARTPOSITION {start_position} MAXRESULTS {PAGE_SIZE}"
        return pd.DataFrame(
            self._client.query(query).json()["QueryResponse"][self._endpoint]
        )

    def _annotate(self, df):
        """
        All quickbooks results have a metadata which is useful to determine the creation time unambiguously
        """
        df["__created_timestamp"] = df["MetaData"].map(lambda x: x.get("CreateTime"))
        df["__updated_timestamp"] = df["MetaData"].map(
            lambda x: x.get("LastUpdatedTime")
        )
        return df

    def iter_all_from(self, start_position=0, request_pages=None):
        """
        Iterate over dataframe chunks
        """
        count = self.get_record_count()
        count -= start_position
        pages = int(np.ceil(count / PAGE_SIZE))

        for i, p in enumerate(range(pages)):
            pos = p * PAGE_SIZE
            if not request_pages or i < request_pages:
                df = self.query_end_point(pos)
                df = self._annotate(df)
                yield df

    def get_records(self, start_position=0, request_pages=None):
        """
        Load all data into a dataframe by iterating over dataframe chunks
        """
        return (
            pd.concat(
                list(self.iter_all_from(start_position, request_pages=request_pages))
            )
            .reset_index()
            .drop("index", 1)
        )

    def __iter__(self):
        """
        Access to all records
        Iterates dataframes and then unpacks them
        """
        for df in self.iter_all_from(start_position=0):
            for record in df.to_dict("records"):
                yield record
