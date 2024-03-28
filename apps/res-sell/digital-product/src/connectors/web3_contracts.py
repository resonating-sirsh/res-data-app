import time
from typing import Any
from web3 import Web3, contract
from web3.middleware.geth_poa import geth_poa_middleware

from src.connectors.bases import ClientBase, ContractBase, ONEContractException


class Web3Client(ClientBase):
    def __init__(self, node_url, private_key) -> None:
        w3 = Web3(Web3.HTTPProvider(node_url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        self._account = w3.eth.account.from_key(private_key)
        w3.eth.default_account = self._account
        for i in range(4):
            if w3.isConnected():
                self._client = w3
            else:
                if i <= 2:
                    time.sleep(3)
                    continue
                raise ONEContractException("Web3 connection fails")

    def get_contract(self, abi, contract_addresss) -> contract.Contract:
        return self._client.eth.contract(
            address=self._client.toChecksumAddress(contract_addresss), abi=abi
        )

    def send_transaction(self, transation: Any):
        account_address = self._account.address
        nonce = self._client.eth.getTransactionCount(account_address)
        transation["nonce"] = nonce
        signed = self._account.signTransaction(transaction_dict=transation)
        hash = self._client.eth.send_raw_transaction(signed.rawTransaction)
        return self._client.eth.wait_for_transaction_receipt(hash)

    def to_eth(self, gas_fee):
        return self._client.fromWei(gas_fee, "ether")


class ONEContract(ContractBase):
    def __init__(self, client: ClientBase, contract_address, abi) -> None:
        self.client = client
        self._contract: contract.Contract = client.get_contract(
            abi,
            contract_address,
        )
        self._client = client._client
        self._account = client._account

    def mint_nft(self, uri: str, token_id: int):
        account_address = self._account.address
        transation = self._contract.functions.mintToken(
            account_address, uri, token_id
        ).buildTransaction({"from": account_address})
        return self.client.send_transaction(transation)

    def token_id_of(self, uri: str):
        tokenId = self._contract.functions.tokenIdOf(uri).call(
            {"from": self._account.address}
        )
        return tokenId

    def transfer_nft(self, token_id, to_address):
        from_address = self._account.address
        transaction = self._contract.functions.transferFrom(
            from_address, self._client.toChecksumAddress(to_address), int(token_id)
        ).buildTransaction({"from": from_address})
        return self.client.send_transaction(transaction)
