import pytest


def test_sql_and_pydantic_fields_PaymentsGetOrUpdateOrder():
    from res.flows.finance import sql_queries
    from schemas.pydantic.payments import PaymentsGetOrUpdateOrder

    from res_tests.res.utils.test_utils import verify_sqlquery_satisfies_pydantic

    import pytest

    verify_sqlquery_satisfies_pydantic(
        sql_query=sql_queries.INSERT_FULLFILMENT_STATUS_HISTORY,
        pydantic_type=PaymentsGetOrUpdateOrder,
        exclude_sql_cols=["id"],
        exclude_pydantic_attribs=["one_number"],
    )


@pytest.mark.skip(reason="Dont run on CI build")
def test_update_order_payload_one_number_only():
    import os

    if "HASURA_ENDPOINT" in os.environ:
        os.environ.pop("HASURA_ENDPOINT")
    os.environ["RES_ENV"] = "production"
    os.environ["HASURA_ENDPOINT"] = "https://hasura.resmagic.io"
    os.environ["KAFKA_SCHEMA_REGISTRY_URL"] = "localhost:8001"
    # PROD
    os.environ[
        "KAFKA_KGATEWAY_URL"
    ] = "https://datadev.resmagic.io/kgateway/submitevent"
    os.environ["RDS_SERVER"] = "localhost:5432"

    from res.flows.finance import sql_queries
    from res.flows.finance.payments_api_utils import update_order
    from schemas.pydantic.payments import PaymentsGetOrUpdateOrder
    import res.connectors
    from res_tests.res.utils.test_utils import verify_sqlquery_satisfies_pydantic

    import pytest
    import uuid

    # Given
    postgres = res.connectors.load("postgres")

    order_details_payload = {
        "process_id": "close_fulfillment_bot" + uuid.uuid4().hex[-4:],
        "shipped_datetime": "2023-10-19T20:50:37.201031+00:00",
        "one_number": 10256858,
        "order_number": "TK-46156",
        "fulfilled_from_inventory": False,
        "updated_at": "2023-10-19T20:50:37.201032+00:00",
    }

    order_details = PaymentsGetOrUpdateOrder(**order_details_payload)

    # When
    result = update_order(order_details)

    # Assert

    assert result is not None
    assert result["id"] is not None
    assert result["order_line_item_id"] is not None

    delete_query = f"DELETE FROM sell.order_item_fulfillments_status_history where id = '{result['id']}' and process_id = '{order_details.process_id}'  "

    postgres.run_update(delete_query)


@pytest.mark.skip(reason="Dont run on CI build")
def test_update_order_payload_sku_and_order_number():
    import os

    if "HASURA_ENDPOINT" in os.environ:
        os.environ.pop("HASURA_ENDPOINT")
    os.environ["RES_ENV"] = "production"
    os.environ["HASURA_ENDPOINT"] = "https://hasura.resmagic.io"
    os.environ["KAFKA_SCHEMA_REGISTRY_URL"] = "localhost:8001"
    # PROD
    os.environ[
        "KAFKA_KGATEWAY_URL"
    ] = "https://datadev.resmagic.io/kgateway/submitevent"
    os.environ["RDS_SERVER"] = "localhost:5432"

    from res.flows.finance import sql_queries
    from res.flows.finance.payments_api_utils import update_order
    from schemas.pydantic.payments import PaymentsGetOrUpdateOrder

    from res_tests.res.utils.test_utils import verify_sqlquery_satisfies_pydantic
    import res.connectors
    import pytest
    import uuid

    postgres = res.connectors.load("postgres")

    # Given
    order_details_payload = {
        "one_number": 0,
        "current_sku": "KT-2011 LTCSL OCTCLW 3ZZMD",
        "order_number": "KT-45694",
        "quantity": 1,
        "shipping_notification_sent_cust_datetime": None,
        "ready_to_ship_datetime": None,
        "shipped_datetime": "2023-09-20T13:58:12.761Z",
        "received_by_cust_datetime": None,
        "process_id": "my_bot_name" + uuid.uuid4().hex[-4:],
        "metadata": {},
        "fulfilled_from_inventory": "True",
        "hand_delivered": "False",
        "updated_at": "2023-09-20T13:58:12.761Z",
    }

    order_details = PaymentsGetOrUpdateOrder(**order_details_payload)

    # When
    result = update_order(order_details)

    # Assert

    assert result is not None
    assert result["id"] is not None
    assert result["order_line_item_id"] is not None

    delete_query = f"DELETE FROM sell.order_item_fulfillments_status_history where id = '{result['id']}' and process_id = '{order_details.process_id}' and order_number = '{order_details.order_number}'"

    postgres.run_update(delete_query)
