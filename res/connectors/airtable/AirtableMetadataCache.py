"""
Module to manage caching data for airtable tables, bases and columns
Should be used by any other class which needs table ids, table metadata, base info, converting between IDs
implements a more robust cache stored in hasura than getting the data from airtable itself
"""
import res


from . import safe_http
from . import formatting
from . import get_airtable_request_header, safe_http


import pandas as pd
import os
from res.utils import secrets
import res
import uuid
import time
import inspect

AIRTABLE_TIMEOUT = None


AIRTABLE_API_URI = "https://api.airtable.com/v0"


UPSERT_AND_DELETE_AIRTABLE_BASE_CACHE = """

 mutation UpsertAndDeleteBases($bases: [infraestructure_airtable_cache_metadata_bases_insert_input!]!, $idList: [uuid!]!) {
  insert_infraestructure_airtable_cache_metadata_bases(
    objects: $bases,
    on_conflict: {
      constraint: airtable_cache_metadata_bases_pkey,
      update_columns: []
         
    }
  ) {
    affected_rows
  }


   delete_infraestructure_airtable_cache_metadata_bases(where: {id: {_nin: $idList}}) {affected_rows}

}
   
   """


UPSERT_AND_DELETE_AIRTABLE_TABLES_IN_BASE_CACHE = """

 mutation UpsertAndDeleteTablesInBase($tablesinbase: [infraestructure_airtable_cache_metadata_tables_in_base_insert_input!]!, $idList: [uuid!]!, $base_id: String = "") {
  insert_infraestructure_airtable_cache_metadata_tables_in_base(
    objects: $tablesinbase,
    on_conflict: {
      constraint: airtable_cache_tables_in_base_pkey,
      update_columns: []
         
    }
  ) {
    affected_rows
  }


   delete_infraestructure_airtable_cache_metadata_tables_in_base(where: {id: {_nin: $idList}, base_id: {_eq:$base_id}}) {affected_rows}

}
   


   """


UPSERT_AND_DELETE_FIELDS_IN_TABLE_AIRTABLE_CACHE = """

 mutation upsertfieldsintable($fieldsintable: [infraestructure_airtable_cache_metadata_table_fields_insert_input!]!, $idList: [uuid!]!, $base_id: String = "") {
  insert_infraestructure_airtable_cache_metadata_table_fields(
    objects: $fieldsintable,
    on_conflict: {
      constraint: airtable_cache_table_fields_pkey,
      update_columns: []
    }
  ) {
    affected_rows
  }


   delete_infraestructure_airtable_cache_metadata_table_fields(where: {id: {_nin: $idList}, base_id: {_eq:$base_id}}) {affected_rows}

}

"""

# GET_STYLE_HEADERS_BY_ID_AND_SIZE = """query get_style_sizes_by_sku($style_id: uuid = "",  $size_code: String = "") {
#   meta_style_sizes(where: {size_code: {_eq: $size_code}, style_id:{_eq: $style_id}}) {
#     metadata
#     id
# """

GET_TABLES_IN_BASE = """
   query GetTablesInBase ($base_id: String = "") {
    infraestructure_airtable_cache_metadata_tables_in_base (where: {base_id: {_eq: $base_id}}){
        base_id
        db_pkid:id
        primary_key_field_id
        table_clean_name
        table_id
  }
}
"""


GET_FIELDS_IN_TABLE = """
   query GetFieldsInTable ($table_id: String = "") {
    infraestructure_airtable_cache_metadata_table_fields (where: {table_id: {_eq: $table_id}}){
        field_id
        field_name
        field_type_airtable
        db_pkid:id
        is_primary_key_field
        last_updated
        table_id
        base_id
  }
}
"""

GET_BASES_AIRTABLECACHE = """
   query MyQuery {
  infraestructure_airtable_cache_metadata_bases {
    db_pkid: id
    clean_name
    base_id
    permissionLevel: permission_level
  }
}
"""


class AirtableMetadataCache:
    _my_instance = None

    @classmethod
    def get_instance(cls):
        """Return the default AirtableMetadataCache."""
        if cls._my_instance is None:
            cls._my_instance = cls()
        return cls._my_instance

    @classmethod
    def get_refreshed_instance(cls):
        """Reset the  AirtableMetadataCache. Useful for testing only."""
        cls._my_instance = cls()
        return cls._my_instance

    def __init__(self):
        """ """
        self.df_bases = None
        self.dict_tables_in_base = {}
        self.dict_fields_in_base = {}
        self.dict_fields_in_table = {}
        self.df_cols_in_tables = None
        for key in ["AIRTABLE_API_KEY"]:
            if key not in os.environ:
                secrets.secrets_client.get_secret(key)

    def reload_cache(
        self,
        base_ids_csv="appUFmlaynyTcQ5wm,apprcULXTWu33KFsh,appV3vIzxq5jNpY7F,appH5S4hIuz99tAjm",
        force_reload_from_airtable=True,
        hasura=None,
    ):
        res.utils.logger.debug(
            f"full AirtableMetadataCache cache reload requested, bases: {base_ids_csv}"
        )

        start_time = time.time()

        hasura = hasura or res.connectors.load("hasura")

        df = self.get_all_base_names(
            force_reload_from_airtable=force_reload_from_airtable,
            force_reload_from_hasura=True,
            hasura=hasura,
        )
        for base_id in base_ids_csv.split(","):
            self.get_tables_in_base(
                base_id=base_id, hasura=hasura, force_reload_from_airtable=True
            )

        totaltables = sum(len(value) for value in self.dict_tables_in_base.values())
        totalfields = sum(len(value) for value in self.dict_fields_in_base.values())

        end_time = time.time()
        execution_time = (end_time - start_time) * 1000

        self.loghit(inspect.currentframe().f_code.co_name, base_ids_csv, "", "True")

        res.utils.logger.debug(
            f"full AirtableMetadataCache cache reload completed. Time MS:{execution_time}. {len(self.df_bases)} bases, {totaltables} tables, {totalfields} fields"
        )

    #

    def get_all_base_names_from_airtable(self):
        res.utils.logger.debug(f"fetching all bases from airtable")

        json_data = safe_http.request_get(
            AIRTABLE_API_URI + "/meta/bases",
            headers=get_airtable_request_header(),
            timeout=AIRTABLE_TIMEOUT,
        ).json()

        data = pd.DataFrame([r for r in json_data["bases"]])
        data["clean_name"] = data["name"].map(formatting.strip_emoji)
        data = data.drop(columns=["name"])
        data = data.rename(columns={"id": "base_id"})
        data["db_pkid"] = data.apply(
            lambda row: self.determine_db_id(row["base_id"] + row["clean_name"]), axis=1
        )

        # data = data.sort_values(by=data.columns)
        data = data.sort_index(axis=1)
        data = data.sort_values(by="base_id")
        data = data.reset_index(drop=True)
        return data

    def get_all_base_names(
        self,
        hasura=None,
        force_reload_from_hasura=False,
        force_reload_from_airtable=False,
    ):
        if (
            self.df_bases is None
            or len(self.df_bases) == 0
            or force_reload_from_airtable
            or force_reload_from_hasura
        ):
            self.df_bases = self.get_all_bases_from_hasura()

            if len(self.df_bases) == 0 or force_reload_from_airtable:
                self.df_bases = self.get_all_base_names_from_airtable()
                self.update_bases_to_hasura(self.df_bases, hasura)

        df_renamed_all_bases = self.df_bases.rename(columns={"base_id": "id"})
        df_renamed_all_bases = df_renamed_all_bases.drop(["db_pkid"], axis=1)
        # we dont save 'name' in hasura, but in case depenedent code looks for it, rather than a crash, hope for a graceful fail (they should be using base_ID or clean_name), we'll provide a copy of cleanname as name
        df_renamed_all_bases["name"] = (
            df_renamed_all_bases["clean_name"].copy().rename("name")
        )

        self.loghit(
            inspect.currentframe().f_code.co_name,
            "",
            "",
            str(force_reload_from_airtable),
        )

        return df_renamed_all_bases

    def get_tables_in_base(
        self,
        base_id,
        hasura=None,
        force_reload_from_hasura=False,
        force_reload_from_airtable=False,
    ):
        if (
            base_id not in self.dict_tables_in_base
            or force_reload_from_hasura
            or force_reload_from_airtable
        ):
            df_all_tables_in_base = self.get_tables_in_bases_from_hasura(
                base_id, hasura
            )
            self.dict_tables_in_base[base_id] = df_all_tables_in_base

            if len(df_all_tables_in_base) == 0 or force_reload_from_airtable:
                self.reload_tables_in_base_from_airtable_and_save_hasura(
                    base_id, hasura
                )

        df_all_tables_in_base = self.dict_tables_in_base[base_id].copy()
        # we dont save 'name' in hasura, but in case depenedent code looks for it, rather than a crash, hope for a graceful fail (they should be using base_ID or clean_name), we'll provide a copy of cleanname as name
        df_all_tables_in_base["name"] = (
            self.dict_tables_in_base[base_id]["table_clean_name"].copy().rename("name")
        )
        df_all_tables_in_base = df_all_tables_in_base.rename(
            columns={"table_clean_name": "clean_name", "table_id": "id"}
        )
        df_all_tables_in_base = df_all_tables_in_base.drop(
            ["db_pkid", "base_id"], axis=1
        )

        self.loghit(
            inspect.currentframe().f_code.co_name,
            base_id,
            "",
            str(force_reload_from_airtable),
        )

        return df_all_tables_in_base

    def loghit(self, funcName, base_id, table_id, force_reload_from_airtable):
        res.utils.logger.metric_node_state_transition_incr(
            table_id,
            base_id,
            force_reload_from_airtable,
            flow=funcName,
            process="AirtableMetadataCache",
        )

    def reload_tables_in_base_from_airtable_and_save_hasura(self, base_id, hasura=None):
        (
            df_all_tables_in_base,
            dict_list_of_fields_per_tables,
            list_of_field_dicts_in_base,
        ) = self.get_all_tables_in_base_from_airtable(base_id)

        self.update_tables_in_bases_to_hasura(df_all_tables_in_base, hasura)
        self.update_fields_in_base_to_hasura(list_of_field_dicts_in_base, hasura)

        self.dict_fields_in_base[base_id] = list_of_field_dicts_in_base
        self.dict_fields_in_table.update(dict_list_of_fields_per_tables)

        self.dict_tables_in_base[base_id] = df_all_tables_in_base

    def get_all_tables_in_base_from_airtable(self, base_id):
        res.utils.logger.debug(
            f"fetching table metadata for all tables in base {base_id}"
        )
        metadata_url = f"{AIRTABLE_API_URI}/meta/bases/{base_id}/tables"
        json_data = safe_http.request_get(
            metadata_url,
            headers=get_airtable_request_header(),
            timeout=AIRTABLE_TIMEOUT,
        ).json()

        dftables = pd.DataFrame([r for r in json_data["tables"]])
        dftables["table_clean_name"] = dftables["name"].map(formatting.strip_emoji)

        dftables["base_id"] = base_id

        dftables = dftables.rename(columns={"id": "table_id"})
        dftables = dftables.rename(columns={"primaryFieldId": "primary_key_field_id"})

        dftables["db_pkid"] = dftables.apply(
            lambda row: self.determine_db_id(
                row["base_id"]
                + row["table_clean_name"]
                + row["table_id"]
                + row["primary_key_field_id"]
            ),
            axis=1,
        )

        list_of_field_dicts_in_base = []

        dict_list_of_fields_in_table = {}

        for index, tablerow in dftables.iterrows():
            list_of_field_dicts_in_table = []
            list_of_fields_in_table = tablerow["fields"]

            for dict_fields in list_of_fields_in_table:
                if "options" in dict_fields:
                    del dict_fields["options"]
                if "description" in dict_fields:
                    del dict_fields["description"]
                dict_fields["table_id"] = tablerow["table_id"]
                dict_fields["base_id"] = tablerow["base_id"]
                dict_fields["field_id"] = dict_fields.pop("id")
                dict_fields["field_name"] = dict_fields.pop("name")
                dict_fields["field_type_airtable"] = dict_fields.pop("type")
                dict_fields["is_primary_key_field"] = (
                    dict_fields["field_id"] == tablerow["primary_key_field_id"]
                )
                dict_fields["db_pkid"] = self.determine_db_id(
                    dict_fields["field_type_airtable"]
                    + dict_fields["field_id"]
                    + dict_fields["field_name"]
                    + str(dict_fields["is_primary_key_field"])
                    + dict_fields["table_id"]
                )
                list_of_field_dicts_in_base.append(dict_fields)
                list_of_field_dicts_in_table.append(dict_fields)

            df_fields_per_table = pd.DataFrame(list_of_field_dicts_in_table)
            df_fields_per_table = df_fields_per_table.sort_index(axis=1)
            df_fields_per_table = df_fields_per_table.sort_values(
                by=["base_id", "table_id", "field_id"]
            )
            df_fields_per_table = df_fields_per_table.reset_index(drop=True)

            dict_list_of_fields_in_table[tablerow["table_id"]] = df_fields_per_table

        # df_fields_in_table = pd.DataFrame(r for r in dict_list_of_fields_in_table.items())

        drop_view_and_fields = True
        if drop_view_and_fields:
            dftables = dftables.drop(columns=["name", "views", "fields"])

        if "description" in dftables.columns:
            dftables = dftables.drop(columns=["description"])

        # dftables = dftables.sort_values(by=dftables.columns)
        dftables = dftables.sort_index(axis=1)
        dftables = dftables.sort_values(by=["base_id", "table_id"])
        dftables = dftables.reset_index(drop=True)

        return dftables, dict_list_of_fields_in_table, list_of_field_dicts_in_base

    def determine_db_id(self, col1, col2):
        newstr = col1 + col2
        strHash = res.utils.hash_of(newstr)
        return str(uuid.UUID(strHash))

    def determine_db_id(self, rowval):
        strHash = res.utils.hash_of(rowval)
        return str(uuid.UUID(strHash))

    def get_table_name(self, base_id, table_id_or_name, clean_name=True):
        """
        Used to resolve table name/clean name if given a table name or table id. throws exception if not found
        """
        data = self.__resolve_table_record_from_id_or_name(base_id, table_id_or_name)
        self.loghit(
            inspect.currentframe().f_code.co_name, base_id, table_id_or_name, "False"
        )
        series = data["name"] if not clean_name else data["clean_name"]
        return series.iloc[0]

    def __resolve_table_record_from_id_or_name(self, base_id, table_id_or_name):
        df_alltables = self.get_tables_in_base(base_id)
        if table_id_or_name[:3] == "tbl":
            df_filtered = df_alltables[df_alltables["id"] == table_id_or_name]
        else:
            df_filtered = df_alltables[df_alltables["clean_name"] == table_id_or_name]

        if len(df_filtered) == 0:
            res.utils.logger.debug(
                f"missing {table_id_or_name} - reloading cache just in case"
            )

            df_alltables = self.get_tables_in_base(
                base_id, force_reload_from_airtable=True
            )

            df_filtered = df_alltables[df_alltables["id"] == table_id_or_name]

            if len(df_filtered) == 0:
                df_filtered = df_alltables[
                    df_alltables["clean_name"] == table_id_or_name
                ]

        if len(df_filtered) == 0:
            raise Exception(
                f"Could not find table {table_id_or_name} in base {base_id} even after reloading the cache"
            )

        return df_filtered.iloc[[0]]

    def resolve_table_id_from_name_or_id(self, base_id, table_id_or_name) -> str:
        df = self.__resolve_table_record_from_id_or_name(base_id, table_id_or_name)
        return df.iloc[0]["id"]

    def get_table_fields(
        self, base_id, table_id, hasura=None, force_reload_from_airtable=False
    ) -> pd.DataFrame:
        """
        returns a dataframe which contains field id,  name, type etc
        """
        if not table_id in self.dict_fields_in_table or force_reload_from_airtable:
            df_fields_in_table = self.get_fields_in_table_from_hasura(
                table_id=table_id, hasura=hasura
            )

            if len(df_fields_in_table) == 0 or force_reload_from_airtable:
                self.reload_tables_in_base_from_airtable_and_save_hasura(
                    base_id, hasura
                )
            else:
                self.dict_fields_in_table[table_id] = df_fields_in_table

            # df_table_info = self.__resolve_table_record_from_id_or_name(base_id, table_id)
            if table_id not in self.dict_fields_in_table:
                raise Exception(
                    f"Error: cannot find fields for {base_id}.{table_id} either in hasura cache or directly from airtable"
                )

        df_fields_in_table = self.dict_fields_in_table[table_id].rename(
            columns={
                "field_name": "name",
                "field_id": "id",
                "field_type_airtable": "type",
                "is_primary_key_field": "is_key",
            }
        )
        df_fields_in_table = df_fields_in_table.drop(
            ["db_pkid", "base_id", "table_id"], axis=1
        )
        self.loghit(
            inspect.currentframe().f_code.co_name,
            base_id,
            table_id,
            force_reload_from_airtable,
        )

        return df_fields_in_table

    def get_airtable_request_header():
        # return "test2"
        return {"Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}"}

    def get_all_bases_from_hasura(self, hasura=None) -> pd.DataFrame:
        h = hasura or res.connectors.load("hasura")

        result = h.tenacious_execute_with_kwargs(GET_BASES_AIRTABLECACHE)
        df = pd.DataFrame(
            result["infraestructure_airtable_cache_metadata_bases"],
            columns=["db_pkid", "clean_name", "base_id", "permissionLevel"],
        )

        df = df.sort_index(axis=1)
        df = df.sort_values(by="base_id")
        df = df.reset_index(drop=True)
        return df

    def get_tables_in_bases_from_hasura(self, base_id, hasura=None) -> pd.DataFrame:
        h = hasura or res.connectors.load("hasura")

        result = h.tenacious_execute_with_kwargs(GET_TABLES_IN_BASE, base_id=base_id)
        df = pd.DataFrame(
            result["infraestructure_airtable_cache_metadata_tables_in_base"],
            columns=[
                "db_pkid",
                "base_id",
                "table_clean_name",
                "table_id",
                "primary_key_field_id",
            ],
        )

        # df.sort_values(by=df.columns)

        df = df.sort_index(axis=1)
        df = df.sort_values(by=["base_id", "table_id"])
        df = df.reset_index(drop=True)

        return df

    def get_fields_in_table_from_hasura(self, table_id, hasura=None) -> pd.DataFrame:
        h = hasura or res.connectors.load("hasura")

        result = h.tenacious_execute_with_kwargs(GET_FIELDS_IN_TABLE, table_id=table_id)
        df = pd.DataFrame(
            result["infraestructure_airtable_cache_metadata_table_fields"],
            columns=[
                "field_id",
                "field_name",
                "field_type_airtable",
                "db_pkid",
                "table_id",
                "is_primary_key_field",
                "base_id",
            ],
        )

        df = df.sort_index(axis=1)
        df = df.sort_values(by=["base_id", "table_id", "field_id"])
        df = df.reset_index(drop=True)

        return df

    def update_tables_in_bases_to_hasura(self, df_all_tables_in_base, hasura=None):
        if len(df_all_tables_in_base) == 0:
            res.utils.logger.debug(
                f"writing tables in base cache info to hasura not attempted, dataframe was empty"
            )
        else:
            base_id = df_all_tables_in_base.iloc[0]["base_id"]
            res.utils.logger.debug(
                f"writing tables in base cache info to hasura for base id {base_id}"
            )

            hasura = hasura or res.connectors.load("hasura")

            list_tables_inbase = []

            for index, row in df_all_tables_in_base.iterrows():
                dict = row.to_dict()

                dict["id"] = dict.pop("db_pkid")
                list_tables_inbase.append(dict)

            list_id_to_keep = list(df_all_tables_in_base["db_pkid"])

            retval = hasura.tenacious_execute_with_kwargs(
                UPSERT_AND_DELETE_AIRTABLE_TABLES_IN_BASE_CACHE,
                tablesinbase=list_tables_inbase,
                idList=list_id_to_keep,
                base_id=base_id,
            )

    def update_fields_in_base_to_hasura(self, list_all_fields_in_base, hasura=None):
        if len(list_all_fields_in_base) == 0:
            res.utils.logger.debug(
                f"writing fields in table to hasura not attempted, list was empty"
            )
        else:
            res.utils.logger.debug(f"writing fields in base cache info to hasura")

            hasura = hasura or res.connectors.load("hasura")

            for dict_fields in list_all_fields_in_base:
                if "db_pkid" in dict_fields:
                    dict_fields["id"] = dict_fields.pop("db_pkid")
                dict_fields["field_desc"] = ""

            list_ids_to_keep = [d["id"] for d in list_all_fields_in_base]

            base_id = list_all_fields_in_base[0]["base_id"]

            retval = hasura.tenacious_execute_with_kwargs(
                UPSERT_AND_DELETE_FIELDS_IN_TABLE_AIRTABLE_CACHE,
                fieldsintable=list_all_fields_in_base,
                idList=list_ids_to_keep,
                base_id=base_id,
            )

    def update_bases_to_hasura(self, df_bases, hasura=None):
        if len(df_bases) == 0:
            res.utils.logger.debug(
                f"writing all bases cache info to hasura not attempted, dataframe was empty"
            )

        res.utils.logger.debug(f"writing all bases in base cache info to hasura")

        hasura = hasura or res.connectors.load("hasura")

        list_bases = []

        for index, row in df_bases.iterrows():
            dict = row.to_dict()
            dict["permission_level"] = dict.pop("permissionLevel")
            dict["id"] = dict.pop("db_pkid")
            list_bases.append(dict)

        list_id_to_keep = list(df_bases["db_pkid"])

        retval = hasura.tenacious_execute_with_kwargs(
            UPSERT_AND_DELETE_AIRTABLE_BASE_CACHE,
            bases=list_bases,
            idList=list_id_to_keep,
        )
        # contracts_failing=contracts_failed,
        # status=status,

    # def update_bases_to_hasura(
    #     sku, contracts_failed, status="Active", hasura=None):
    # if len(contracts_failed):
    #     status = "Failed"
    # hasura = hasura or res.connectors.load("hasura")
    # return hasura.execute_with_kwargs(
    #     UPSERT_STYLE_STATUS_BY_SKU,
    #     metadata_sku={"sku": sku},
    #     contracts_failing=contracts_failed,
    #     status=status,
    # )


#    jsonbases = """
#         [
#         {
#         "id":"2a83d55b-789a-4143-8d75-3f96fa0ff111",
#         "base_id": "mybaseid1252",
#         "clean_name": "my clean name1252",
#         "permission_level": "admin"
#         },
#         {
#         "id":"2a83d55b-789a-4143-8d75-3f96fa0ff112",
#         "base_id": "mybaseid1252",
#         "clean_name": "my clean name1251",
#         "permission_level": "admin"
#         }

#         ]

#         """

#         jsonidlist = """


#          ["2a83d55b-789a-4143-8d75-3f96fa0ff111", "2a83d55b-789a-4143-8d75-3f96fa0ff112"]

#         """


#         dictbases = json.loads(jsonbases)
#         dictids = json.loads(jsonidlist)
