from abc import ABC, abstractmethod
from typing import Any

from web3 import Web3, contract


class ClientBase(ABC):
    _account: Any
    _client: Web3

    @abstractmethod
    def get_contract(self, abi: str, contract_address: str) -> contract.Contract:
        raise NotImplementedError()

    @abstractmethod
    def send_transaction(self, transation: Any):
        print("transation", transation)
        raise NotImplementedError()


class ContractBase(ABC):
    def __init__(self, client: ClientBase) -> None:
        self.client = client

    @abstractmethod
    def mint_nft(self, uri: str, token_id: str):
        raise NotImplementedError()

    @abstractmethod
    def token_id_of(self, uri: str):
        raise NotImplementedError()

    @abstractmethod
    def transfer_nft(self, token_id: str, to_address: str):
        raise NotImplementedError()


class ONEContractException(Exception):
    def __init__(self, msg, *args: object) -> None:
        super().__init__(msg, *args)
