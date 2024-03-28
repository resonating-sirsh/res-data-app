## Queries

#### Updating Pieces
Set certain Piece fields while nulling out the edes to `point` nodes

``` graphql
mutation updatePiece($input: UpdatePieceInput!) {
  updatePiece(input: $input) {
    numUids
    piece {
      name
      points {
        name
      }
    }
  }
}
```

``` json
{
    "input": {
        "set": {
            "name": "my test square skirt3",
            "key": "skirt"
        },
        "filter": {
            "id": "0xc1a7e"
        },
      	"remove": {
          "points": null
        }

    }
}


```



#### Inserting Pieces

``` graphql
mutation postPiece($pieces:  [AddPieceInput!]!) {
  addPiece(input: $pieces){
    numUids
    piece {
      name
      points {
        resIndex
        name
        point {
          latitude
          longitude
        }
      }
    }
  }
}
```

``` json
{"pieces":
 [{
   "key": "skirt",
   "key": "my test square skirt",
   "points": [
     {"name": "BL", "resIndex": "1", "point":{"latitude":0, "longitude": 0}},
     {"name": "TL", "resIndex": "2", "point":{"latitude":0, "longitude": 1}},
     {"name": "TR", "resIndex": "3", "point":{"latitude":1, "longitude": 1}},
     {"name": "BR", "resIndex": "4", "point":{"latitude":1, "longitude": 0}}
   ]
 }]
}
```
