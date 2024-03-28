import pytest
from schemas.pydantic.meta import BodyMetaOneResponse, BodyMetaOneRequest
from res.flows.meta.ONE.meta_one import BodyMetaOne
from res.flows.meta.ONE.body_node import BodyMetaOneNode


@pytest.mark.service
def test_body_meta_one_transition():
    """
    this is just an integration test poorly mocked - needs to be on hasura prod at present
    the point of this is to test some tricky moving parts and airtable deps used to create state
    """
    bm = BodyMetaOne("TT-4032", 1, "2ZZSM")

    bor = None
    b = BodyMetaOneNode.get_body_as_request(bm.body_code)
    bor = b["body_one_ready_request"]
    b.pop("pieces")
    typed_record = BodyMetaOneRequest(**b)

    response = bm.status_at_node(
        sample_size=typed_record.sample_size,
        request=typed_record,
        object_type=BodyMetaOneResponse,
        bootstrap_failed=typed_record.contracts_failed,
        contract_failure_context=typed_record.contract_failure_context,
        body_request_id=bor,
    )
    response

    r = BodyMetaOneNode.move_body_request_to_node_for_contracts(
        response.body_one_ready_request, ["SUSPICIOUS_PIECE_SHAPE"], plan=True
    )

    assert r.get("Apply 3D Standards Grade POMs_Status") == "Failed"
