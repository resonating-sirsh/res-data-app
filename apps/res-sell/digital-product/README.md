# ONE.DigitalProduct API

ONE.DigitalProduct API is service which connect with Resonance Smart Contract deploy on the ethereum network to generate NFT for 3D assets based on existing styles on create.ONE.

---------

## Getting Started

1- Make sure that you have installed all of the package on `res-data`

```bash
pip install -r ../../res/docker/res-data/requirements.txt
```

2- Run server

```bash
python ./src/main.py 
```

3- To run the test

```bash
pytest .
```

---------

## DigitalProduct API

Endpoint where capture all the event from Hasura NFTRequest table.

```http
POST /webhook 
Content-Type: application/json
Authorization: Beare {{ JWT_TOKEN }}

--HASURA_EVENT_PAYLOAD--
```

[Hasura Event payload](https://hasura.io/docs/latest/graphql/core/event-triggers/payload/)
