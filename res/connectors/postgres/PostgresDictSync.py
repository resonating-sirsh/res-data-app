from typing import List, Optional, Union

from res.connectors.postgres import PostgresConnector
from res.connectors.postgres.PostgresConnector import PostgresConnector
from res.connectors.postgres.utils import (
    build_insert_columns,
    build_insert_values,
    build_json_column,
    build_set_clause,
    quote_value,
)
from res.utils import logger
from res.utils.dicts import index_dicts, traverse_dict


class PostgresDictSync:
    """
    Syncs records provided as a list of dictionaries to a target table
    in Postgres.

    Example:
        PostgresDictSync(
            "target_table",
            [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}],
            on="id",
            pg=PostgresConnector(),
        ).sync()
    """

    def diff_dicts(self, dict1, dict2):
        diffs = {}
        for key in dict1.keys():
            if key not in dict2 or dict1[key] != dict2[key]:
                diffs[key] = (dict1.get(key), dict2.get(key))
        for key in dict2.keys():
            if key not in dict1 or dict1[key] != dict2[key]:
                diffs[key] = (dict1.get(key), dict2.get(key))
        return diffs

    def __init__(
        self,
        table: str,
        source_records: List[dict],
        on: Optional[Union[str, dict, tuple, list]] = None,
        source_on: Optional[Union[str, dict, tuple, list]] = None,
        db_on: Optional[Union[str, dict, tuple, list]] = None,
        pg: Optional[PostgresConnector] = None,
        ignore_columns_for_update: List[str] = [],
    ):
        """
        Args:
            table: the table to sync to
            source_records: the records to sync
            on: the path to use for syncing, assuming its the same
                in the source and the database
            source_on: the path to use for syncing in the source records
            db_on: the path to use for syncing in the database
            pg: the PostgresConnector to use
        """
        self.table = table
        self.source_records = source_records
        self.pg = pg or PostgresConnector()
        if on:
            self.db_sync_path = on
            self.source_sync_path = on
        else:
            self.db_sync_path = db_on
            self.source_sync_path = source_on

        db_records = self.pg.run_query(f"SELECT * FROM {self.table}").to_dict("records")
        db_index = index_dicts(
            db_records, lambda x: traverse_dict(x, self.db_sync_path)
        )

        source_index = index_dicts(
            source_records, lambda x: traverse_dict(x, self.source_sync_path)
        )

        record_ids_to_add = []
        record_ids_to_delete = []
        record_ids_to_update = []

        for key in source_index.keys():
            if key not in db_index:
                record_ids_to_add.append(key)

        for key in db_index.keys():
            if key not in source_index:
                record_ids_to_delete.append(key)

        ignore_columns = ignore_columns_for_update

        for key in source_index.keys():
            if key not in db_index:
                continue
            if key in record_ids_to_add:
                continue
            source_record = source_index[key]
            db_record = db_index[key]
            for k in ignore_columns:
                if k in db_record:
                    del db_record[k]
                if k in source_record:
                    del source_record[k]

            if source_record != db_record:

                myres = self.diff_dicts(source_record, db_record)
                record_ids_to_update.append(key)

        self.db_index = db_index
        self.source_index = source_index
        self.record_ids_to_add = record_ids_to_add
        self.record_ids_to_delete = record_ids_to_delete
        self.record_ids_to_update = record_ids_to_update

    def sync_additions(self):
        if not self.record_ids_to_add:
            return
        records = []
        for record_id in self.record_ids_to_add:
            records.append(self.source_index[record_id])
        logger.info(f"Syncing {len(records)} additions to {self.table}")
        insert_columns = build_insert_columns(records)
        insert_values = build_insert_values(records)
        self.pg.run_update(
            f"INSERT INTO {self.table} {insert_columns} VALUES {insert_values}"
        )

    def sync_deletions(self):
        if not self.record_ids_to_delete:
            return

        sync_column = build_json_column(self.db_sync_path)

        logger.info(
            f"Syncing {len(self.record_ids_to_delete)} deletions to {self.table}"
        )
        record_ids = ", ".join([quote_value(x) for x in self.record_ids_to_delete])
        self.pg.run_update(
            f"DELETE FROM {self.table} WHERE {sync_column} IN ({record_ids})"
        )

    def sync_updates(self):
        if not self.record_ids_to_update:
            return
        records = []
        for record_id in self.record_ids_to_update:
            records.append(self.source_index[record_id])

        logger.info(f"Syncing {len(records)} updates to {self.table}")
        sync_column = build_json_column(self.db_sync_path)
        for record in records:
            update_values = build_set_clause(record)
            sync_value = quote_value(traverse_dict(record, self.db_sync_path))
            self.pg.run_update(
                f"UPDATE {self.table} SET {update_values} WHERE {sync_column} = {sync_value}"
            )

    def sync(self):
        self.sync_additions()
        self.sync_deletions()
        self.sync_updates()
