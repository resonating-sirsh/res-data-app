import pytest
from src.connectors.web3_contracts import ONEContract
from src.connectors.bases import ClientBase


class FakeWeb3Client(ClientBase):
    def __init__(self):
        account = type("Account", (), {})
        account.address = "0xthis_is_a_fake_address"
        self._account = account
        self._client = None

    def get_contract(self, abi: str, contract_address: str):
        contract = type("contract.Contract", (), {})
        contract.functions = type("", (), {})

        def mintToken(*args, **kargs):
            nested_object = type("", (), {})
            nested_object.buildTransaction = lambda x: x
            return nested_object

        contract.functions.mintToken = mintToken
        return contract

    def send_transaction(self, transation):
        return transation


class TestONEContract:
    @pytest.fixture
    def fake_web3_client(self):
        return FakeWeb3Client()

    def test_mint_nft(self, fake_web3_client):
        fake_uri = "http://ipfs.io/ipfs/fake_uri_here"
        one_contract = ONEContract(fake_web3_client, "FAKE_ADDRESS", {})
        one_contract.mint_nft(fake_uri, 1)
