from res.connectors.airtable.AirtableMetadataCache import AirtableMetadataCache
from res.connectors.airtable.AirtableConnector import AirtableConnector
import res
import os
import res.connectors.airtable
import pytest


@pytest.mark.slow
@pytest.mark.service
def test_cache_reload(capsys):
    os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/graphql"

    cache = AirtableMetadataCache.get_refreshed_instance()
    cache.reload_cache(
        base_ids_csv="appUFmlaynyTcQ5wm,apprcULXTWu33KFsh,appV3vIzxq5jNpY7F,appH5S4hIuz99tAjm"
    )
    captured = capsys.readouterr()  # capture stdout and stderr

    assert "fetching all bases from airtable" in captured.out
    assert (
        "writing tables in base cache info to hasura for base id appUFmlaynyTcQ5wm"
        in captured.out
    )
    assert (
        "writing tables in base cache info to hasura for base id apprcULXTWu33KFsh"
        in captured.out
    )
    assert (
        "writing tables in base cache info to hasura for base id appV3vIzxq5jNpY7F"
        in captured.out
    )
    assert (
        "writing tables in base cache info to hasura for base id appH5S4hIuz99tAjm"
        in captured.out
    )
    assert (
        "fetching table metadata for all tables in base appUFmlaynyTcQ5wm"
        in captured.out
    )
    assert (
        "fetching table metadata for all tables in base apprcULXTWu33KFsh"
        in captured.out
    )
    assert (
        "fetching table metadata for all tables in base appV3vIzxq5jNpY7F"
        in captured.out
    )
    assert (
        "fetching table metadata for all tables in base appH5S4hIuz99tAjm"
        in captured.out
    )
    assert "full AirtableMetadataCache cache reload completed." in captured.out
    assert (
        "full AirtableMetadataCache cache reload requested, bases: appUFmlaynyTcQ5wm,apprcULXTWu33KFsh,appV3vIzxq5jNpY7F,appH5S4hIuz99tAjm"
        in captured.out
    )


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_get_tables_in_base():

    os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/graphql"

    cache = AirtableMetadataCache.get_refreshed_instance()
    baseid = "apprcULXTWu33KFsh"  #'appUFmlaynyTcQ5wm' 'appjmzNPXOuynj6xP'

    dfs = []
    dfs.append(cache.get_tables_in_base(baseid))

    dfs.append(cache.get_tables_in_base(baseid, force_reload_from_hasura=True))
    dfs.append(cache.get_tables_in_base(baseid, force_reload_from_airtable=True))
    dfs.append(cache.get_tables_in_base(baseid))
    dfs.append(cache.get_tables_in_base(baseid))
    dfs.append(cache.get_tables_in_base(baseid, force_reload_from_hasura=True))
    cache = AirtableMetadataCache.get_refreshed_instance()

    dfs.append(cache.get_tables_in_base(baseid))
    dfs.append(cache.get_tables_in_base(baseid, force_reload_from_airtable=True))
    dfs.append(cache.get_tables_in_base(baseid, force_reload_from_hasura=True))
    dfs.append(cache.get_tables_in_base(baseid))

    # cant add bad hasura url tests as cache throws exceptoin, which breaks this test, but is what airtable connector expects and needs for it to revert to hasura
    # os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/graphqlNoSuchUrl"

    cache = AirtableMetadataCache.get_refreshed_instance()

    dfs.append(cache.get_tables_in_base(baseid))
    dfs.append(cache.get_tables_in_base(baseid, force_reload_from_airtable=True))
    dfs.append(cache.get_tables_in_base(baseid, force_reload_from_hasura=True))
    dfs.append(cache.get_tables_in_base(baseid))

    # todo add bad hasura URL

    arrBools = []
    for i in range(len(dfs)):
        arrBools.append(dfs[1].equals(dfs[i]))

    assert all(arrBools)


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_get_fields_in_table():
    def exec_get_fields_in_table(hasuraUrl):
        AirtableMetadataCache.get_refreshed_instance()
        os.environ["HASURA_ENDPOINT"] = hasuraUrl
        cache = res.connectors.airtable.AirtableMetadataCache.get_instance()

        hasura = res.connectors.load("hasura")

        dstablesInBase = cache.get_tables_in_base("appUFmlaynyTcQ5wm")
        fields = []
        fields.append(
            cache.get_table_fields(
                "apprcULXTWu33KFsh",
                "tblwDQtDckvHKXO4w",
                force_reload_from_airtable=True,
                hasura=hasura,
            )
        )
        #
        fields.append(
            cache.get_table_fields(
                "apprcULXTWu33KFsh",
                "tblwDQtDckvHKXO4w",
                force_reload_from_airtable=True,
                hasura=hasura,
            )
        )
        fields.append(
            cache.get_table_fields(
                "apprcULXTWu33KFsh", "tblwDQtDckvHKXO4w", hasura=hasura
            )
        )

        fields.append(
            cache.get_table_fields(
                "apprcULXTWu33KFsh",
                "tblwDQtDckvHKXO4w",
                force_reload_from_airtable=True,
                hasura=hasura,
            )
        )
        fields.append(
            cache.get_table_fields(
                "apprcULXTWu33KFsh", "tblwDQtDckvHKXO4w", hasura=hasura
            )
        )

        fields2 = []
        fields2.append(
            cache.get_table_fields(
                "appUFmlaynyTcQ5wm",
                "tblvs7XeydmZquUPR",
                hasura=hasura,
                force_reload_from_airtable=True,
            )
        )

        fields2.append(
            cache.get_table_fields(
                "appUFmlaynyTcQ5wm", "tblvs7XeydmZquUPR", hasura=hasura
            )
        )
        fields2.append(
            cache.get_table_fields(
                "appUFmlaynyTcQ5wm", "tblvs7XeydmZquUPR", hasura=hasura
            )
        )

        # pause test here, go to airtable, add a col to this table
        fieldsNewInAirtable = []
        fieldsNewInAirtable.append(
            cache.get_table_fields(
                "appUFmlaynyTcQ5wm",
                "tblvs7XeydmZquUPR",
                force_reload_from_airtable=True,
            )
        )
        fieldsNewInAirtable.append(
            cache.get_table_fields("appUFmlaynyTcQ5wm", "tblvs7XeydmZquUPR")
        )

        diffsExist = False
        for df1 in fields:
            if not df1.equals(fields[0]):
                diffsExist = True

        diffsExist2 = False
        for df1 in fields2:
            if not df1.equals(fields2[0]):
                diffsExist2 = True

        diffsExist3 = False
        for df1 in fieldsNewInAirtable:
            if not df1.equals(fieldsNewInAirtable[0]):
                diffsExist3 = True

        equalsAfterFieldAdd = (
            fieldsNewInAirtable[0]
            .reset_index(drop=True)
            .equals(fields2[0].reset_index(drop=True))
        )

        assert diffsExist == False
        assert diffsExist2 == False
        assert diffsExist3 == False
        assert (
            equalsAfterFieldAdd == True
        )  # This will only be true if you manually add an airtable field - change this to equalsAfterFieldAdd == False if you do

    exec_get_fields_in_table("http://localhost:8081/v1/graphql")
    # cant call the cache directly with bad hasura URL - thats what airtableconnector tests of the same func are for
    # exec_get_fields_in_table("http://localhost:8081/v1/graphqlNoSUchUrl")


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_airtableConnector_integration_get_all_bases():
    AirtableMetadataCache.get_refreshed_instance()
    os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/graphql"
    dsBasesHasura1 = AirtableConnector.get_base_names(try_from_cache=True)
    dsBasesAirtable1 = AirtableConnector.get_base_names(try_from_cache=False)

    dsBasesAirtableThruMetadataCache1 = AirtableConnector.get_base_names(
        try_from_cache=True, force_reload_from_airtable_metadatacache=True
    )

    AirtableMetadataCache.get_refreshed_instance()
    os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/NoSuchUrl"
    dsBasesHasura2 = AirtableConnector.get_base_names(try_from_cache=True)
    dsBasesAirtable2 = AirtableConnector.get_base_names(try_from_cache=False)
    dsBasesAirtableThruMetadataCache2 = AirtableConnector.get_base_names(
        try_from_cache=True, force_reload_from_airtable_metadatacache=True
    )

    def sort_fix_up_df(df):
        df.sort_index(axis=1, inplace=True)
        df.sort_values(by="id", inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.drop(columns=["name"], errors="ignore", inplace=True)

    sort_fix_up_df(dsBasesHasura1)
    sort_fix_up_df(dsBasesHasura2)
    sort_fix_up_df(dsBasesAirtable1)
    sort_fix_up_df(dsBasesAirtable2)
    sort_fix_up_df(dsBasesAirtableThruMetadataCache2)
    sort_fix_up_df(dsBasesAirtableThruMetadataCache1)

    # 'name' series is just a renamed copy of clean_name, as we are not storing in hasura. so if we include name in the comparison operations, these asserts will fail. so its being dropped in teh compare.

    assert dsBasesHasura1.equals(dsBasesHasura2)

    assert dsBasesHasura1.equals(dsBasesAirtable1)

    assert dsBasesHasura1.equals(dsBasesAirtable2)
    assert dsBasesHasura1.equals(dsBasesAirtableThruMetadataCache1)
    assert dsBasesHasura1.equals(dsBasesAirtableThruMetadataCache2)


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_airtableConnector_integration_get_table_name():
    AirtableMetadataCache.get_refreshed_instance()
    os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/graphql"
    airtableconn = res.connectors.load("airtable")

    base = airtableconn["appUFmlaynyTcQ5wm"]
    table_name = base.get_table_name("tblvs7XeydmZquUPR")
    table_name2 = base.get_table_name("Damo Test Rolls")

    base = airtableconn["apprcULXTWu33KFsh"]
    table_name3 = base.get_table_name("tblwDQtDckvHKXO4w")
    table_name4 = base.get_table_name("Print Assets (flow)")

    assert table_name.equals(table_name2)
    assert table_name3.equals(table_name4)


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_airtableConnector_integration_get_table_name_bad_hasura():
    AirtableMetadataCache.get_refreshed_instance()
    os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/NoSuchUrl"
    airtableconn = res.connectors.load("airtable")

    base = airtableconn["appUFmlaynyTcQ5wm"]
    table_name = base.get_table_name("tblvs7XeydmZquUPR")
    table_name2 = base.get_table_name("Damo Test Rolls")

    base = airtableconn["apprcULXTWu33KFsh"]
    table_name3 = base.get_table_name("tblwDQtDckvHKXO4w")
    table_name4 = base.get_table_name("Print Assets (flow)")

    assert table_name == table_name2
    assert table_name3 == table_name4


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_airtableConnector_integration_get_table_fields():
    def exec_table_fields_test(hasuraUrl):

        AirtableMetadataCache.get_refreshed_instance()

        os.environ["HASURA_ENDPOINT"] = hasuraUrl
        airtableconn = res.connectors.load("airtable")

        base = airtableconn["appUFmlaynyTcQ5wm"]

        hasuraCache1 = base.get_table_fields(
            "tblvs7XeydmZquUPR",
            try_from_cache=True,
            use_airtablemetadatacache_first=True,
        )
        hasuraThruAirtableCache1 = base.get_table_fields(
            "tblvs7XeydmZquUPR",
            try_from_cache=False,
            use_airtablemetadatacache_first=True,
        )
        airtabledirect1 = base.get_table_fields(
            "tblvs7XeydmZquUPR",
            try_from_cache=False,
            use_airtablemetadatacache_first=False,
        )

        base = airtableconn["apprcULXTWu33KFsh"]
        hasuraCache2 = base.get_table_fields(
            "tblwDQtDckvHKXO4w",
            try_from_cache=True,
            use_airtablemetadatacache_first=True,
        )
        hasuraThruAirtableCache2 = base.get_table_fields(
            "tblwDQtDckvHKXO4w",
            try_from_cache=False,
            use_airtablemetadatacache_first=True,
        )
        airtabledirect2 = base.get_table_fields(
            "tblwDQtDckvHKXO4w",
            try_from_cache=False,
            use_airtablemetadatacache_first=False,
        )

        def sort_field_df(df):
            df.drop(columns=["description"], errors="ignore", inplace=True)
            df.sort_index(axis=1, inplace=True)
            df.sort_values(by="id", inplace=True)
            df.reset_index(drop=True, inplace=True)

        def dataframe_are_same_apart_from_colum_order(df1, df2):
            same = df1.sort_index(axis=1).equals(df2.sort_index(axis=1))
            return same

        airtabledirect1 = airtabledirect1.drop(["description"], axis=1)
        airtabledirect2 = airtabledirect2.drop(["description"], axis=1)

        sort_field_df(hasuraCache1)
        sort_field_df(hasuraThruAirtableCache1)
        sort_field_df(airtabledirect1)
        sort_field_df(hasuraCache2)
        sort_field_df(hasuraThruAirtableCache2)
        sort_field_df(airtabledirect2)

        assert dataframe_are_same_apart_from_colum_order(hasuraCache1, airtabledirect1)

        assert dataframe_are_same_apart_from_colum_order(
            hasuraCache2, hasuraThruAirtableCache2
        )

        assert dataframe_are_same_apart_from_colum_order(hasuraCache2, airtabledirect2)

    exec_table_fields_test("http://localhost:8081/v1/graphql")
    exec_table_fields_test("http://localhost:8081/v1/SimulateBrokenUrl")


# test connector.get_table_id_from_name

# _get_base_tables_metadata, reloadcache test this, get_base_tables


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_airtableConnector_integration_reload_cache():
    def exec_reload_cache_test(table_id, base_id, hasuraUrl):

        AirtableMetadataCache.get_refreshed_instance()
        os.environ["HASURA_ENDPOINT"] = hasuraUrl
        airtableconn = res.connectors.load("airtable")

        base = airtableconn[base_id]

        def sort_field_df(df):
            df.sort_index(axis=1, inplace=True)
            df.sort_values(by="id", inplace=True)
            df.reset_index(drop=True, inplace=True)

        dfbases = []
        dfbases.append(
            base.reload_cache(
                table_id=table_id, use_airtablemetadatacache_first=False
            ).drop(columns=["description", "primary_key_field_id"], errors="ignore")
        )
        dfbases.append(
            base.reload_cache(
                table_id=table_id, use_airtablemetadatacache_first=True
            ).drop(columns=["description", "primary_key_field_id"], errors="ignore")
        )
        dfbases.append(
            base.reload_cache(
                table_id=table_id, use_airtablemetadatacache_first=False
            ).drop(columns=["description", "primary_key_field_id"], errors="ignore")
        )
        dfbases.append(
            base.reload_cache(
                table_id=table_id, use_airtablemetadatacache_first=True
            ).drop(columns=["description", "primary_key_field_id"], errors="ignore")
        )
        dfbases.append(
            base.reload_cache(
                table_id=table_id, use_airtablemetadatacache_first=True
            ).drop(columns=["description", "primary_key_field_id"], errors="ignore")
        )

        arrBools = []
        for i in range(len(dfbases)):
            sort_field_df(dfbases[i])
            arrBools.append(dfbases[0].equals(dfbases[i]))

        assert all(arrBools)

    exec_reload_cache_test(
        "tblvs7XeydmZquUPR", "appUFmlaynyTcQ5wm", "http://localhost:8081/v1/graphql"
    )
    exec_reload_cache_test(
        "tblvs7XeydmZquUPR",
        "appUFmlaynyTcQ5wm",
        "http://localhost:8081/v1/SimulateBrokenHasura",
    )

    exec_reload_cache_test(
        "tblwDQtDckvHKXO4w", "apprcULXTWu33KFsh", "http://localhost:8081/v1/graphql"
    )
    exec_reload_cache_test(
        "tblwDQtDckvHKXO4w",
        "apprcULXTWu33KFsh",
        "http://localhost:8081/v1/SimulateBrokenHasura",
    )

    exec_reload_cache_test(
        None, "appUFmlaynyTcQ5wm", "http://localhost:8081/v1/graphql"
    )
    exec_reload_cache_test(
        None, "appUFmlaynyTcQ5wm", "http://localhost:8081/v1/SimulateBrokenHasura"
    )


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_airtableConnector_integration_get_table():

    os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/graphql"
    airtableconn = res.connectors.load("airtable")

    base = airtableconn["apprcULXTWu33KFsh"]
    AirtableMetadataCache.get_refreshed_instance()

    def sort_field_df(df):
        df.sort_index(axis=1, inplace=True)
        df.sort_values(by="id", inplace=True)
        df.reset_index(drop=True, inplace=True)

    dfbases = []
    dfbases.append(
        base.get_table(table="tblwDQtDckvHKXO4w").drop(
            columns=["is_key"], errors="ignore"
        )
    )
    dfbases.append(
        base.get_table(table="Print Assets (flow)").drop(
            columns=["is_key"], errors="ignore"
        )
    )

    os.environ["HASURA_ENDPOINT"] = "http://localhost:8081/v1/NoSuchUrl"

    AirtableMetadataCache.get_refreshed_instance()
    df = base.get_table(table="tblwDQtDckvHKXO4w").drop(
        columns=["description", "options"], errors="ignore"
    )
    dfbases.append(df)
    df = base.get_table(table="Print Assets (flow)").drop(
        columns=["description", "options"], errors="ignore"
    )
    dfbases.append(df)

    arrBools = []
    for i in range(len(dfbases)):
        sort_field_df(dfbases[i])
        arrBools.append(dfbases[0].equals(dfbases[i]))

    assert all(arrBools)


@pytest.mark.slow
@pytest.mark.data
@pytest.mark.service
def test_airtableConnector_integration_get_base_tables():

    # takes up to 2 mins to run

    # dsTables = base.get_base_tables()
    # base._get_base_tables_metadata()

    def exec_get_base_tables_test(base_id, hasuraUrl):

        AirtableMetadataCache.get_refreshed_instance()
        os.environ["HASURA_ENDPOINT"] = hasuraUrl
        airtableconn = res.connectors.load("airtable")

        base = airtableconn[base_id]

        def sort_field_df(df):
            df.sort_index(axis=1, inplace=True)
            df.sort_values(by="id", inplace=True)
            df.reset_index(drop=True, inplace=True)

        dfbases = []
        dfbases.append(
            base.get_base_tables(use_cache=True).drop(
                columns=["primary_key_field_id"], errors="ignore"
            )
        )
        dfbases.append(
            base.get_base_tables(use_cache=False).drop(
                columns=["primary_key_field_id"], errors="ignore"
            )
        )
        dfbases.append(
            base.reload_cache(table_id=None, use_airtablemetadatacache_first=True).drop(
                columns=["primary_key_field_id"], errors="ignore"
            )
        )
        dfbases.append(
            base.reload_cache(
                table_id=None, use_airtablemetadatacache_first=False
            ).drop(columns=["primary_key_field_id"], errors="ignore")
        )
        dfbases.append(
            AirtableConnector()
            ._get_base_tables_metadata(
                base_id=base_id,
                try_from_cache=False,
                try_from_airtablemetadatacache_first=True,
            )
            .drop(columns=["primary_key_field_id"], errors="ignore")
        )
        dfbases.append(
            AirtableConnector()
            ._get_base_tables_metadata(
                base_id=base_id,
                try_from_cache=False,
                try_from_airtablemetadatacache_first=False,
            )
            .drop(columns=["primary_key_field_id"], errors="ignore")
        )
        dfbases.append(
            AirtableConnector()
            ._get_base_tables_metadata(
                base_id=base_id,
                try_from_cache=True,
                try_from_airtablemetadatacache_first=True,
            )
            .drop(columns=["primary_key_field_id"], errors="ignore")
        )
        dfbases.append(
            base.get_base_tables(use_cache=True).drop(
                columns=["primary_key_field_id"], errors="ignore"
            )
        )
        dfbases.append(
            base.reload_cache(table_id=None, use_airtablemetadatacache_first=True).drop(
                columns=["primary_key_field_id"], errors="ignore"
            )
        )

        arrBools = []
        for i in range(len(dfbases)):
            sort_field_df(dfbases[i])
            arrBools.append(dfbases[0].equals(dfbases[i]))

        assert all(arrBools)

    exec_get_base_tables_test("appUFmlaynyTcQ5wm", "http://localhost:8081/v1/graphql")
    exec_get_base_tables_test(
        "appUFmlaynyTcQ5wm", "http://localhost:/v1/SimulateBrokenHasura"
    )

    exec_get_base_tables_test("apprcULXTWu33KFsh", "http://localhost:8081/v1/graphql")
    exec_get_base_tables_test(
        "apprcULXTWu33KFsh", "http://localhost:/v1/SimulateBrokenHasura"
    )
