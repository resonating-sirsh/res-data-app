# Make.ONE


The Meta.ONE objects are used to managed bodies and styles. Both the CRUD and the nodes that process them

## Notes on the API


## Preparing to make

The CRUD can be used to pull assets on demand. However we use the prepare (print) pieces node to select the assets we need and run gates that check make assets. This is explained below.

 We cache all annotation on styles once per style - these can be cached ahead of time or on demand. We can also invalidate the cache with a bump of schema version. We use the prep print piece response to send the messages that are going to be consumed downstream so we do not need to restore anything else from a caching perspective.  A consumer such as nesting can receive the events and handle them. By keeping the contract on the make side, having cached all style relevant data, we can add further dynamic data in the make context in this node.

The attributes in the PPP piece payload can be used by the make system for nesting etc and includes attributes such as healing or make instance of the piece. An example schema is shown below.

A special annotation-json is predetermined and reserves a space for a ONE label on printed pieces - everything about the style without this "make context" is cached once per style. Then we can use the ONE number and the rest of the context to add the label with one number, the healing context and other piece info;
- the one number
- is it an extra or healing piece
- what is the piece type 
- how many pieces make up the set and what is the piece index 

### Prep print pieces request
- There is a pydantic type for the pre print pieces called `PrepPrintAssetRequest`. This schema can be used to validate what we send to kafka topic `res_meta.dxa.prep_pieces_requests`
- From this we generate a response object of type `PrepPieceResponse`. This schema cane be used to validate what we send to the response kafka topic `res_meta.dxa.prep_pieces_responses`

For example a piece payload for this is shown below which has everything needed to nested. We pack run time context such as one number or other piece info not in the schema into the annotation so that there is a self contained object that can be used to label a piece. 


```python
class PrepPrintPieces(FlowApiModel):
    """
    keys
    """
    piece_id: str
    asset_id: str
    asset_key: str
    piece_name: str
    piece_code: str
    meta_piece_key: str
    make_sequence: int = Field(default=0)
    """
    uris
    """
    filename: str
    outline_uri: str
    cutline_uri: str
    artwork_uri: str
    """
    added piece info
    """
    multiplicity: str = Field(default=1)
    requires_block_buffer: bool = Field(default=False)
    piece_area_yards: Optional[float]
    fusing_type: Optional[str]
    annotation_json: Optional[str]
```

### Modes for requests

1. The default mode will use the old prep print pieces functionality
2. Setting `flow=v2` will run the new cache based functionality - downstream processes need to be changed to add the annotation 
3. Setting `flow=cache_only` runs a subset of functionality to `v2` by just running the caching part without any other side effects such as airtable updates or kafka events 

 