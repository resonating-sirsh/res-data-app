# Graph API Data Integration

## Overview
This application contains code for a Python file that is used to query the 
resmagic.io GraphQL API ("Graph") on a daily basis. Each style saved within the 
Graph has pricing information recorded by size; each size has a variety of price
components such as overhead, labor, sewing, and print cost.  

Costs for each style and its sizes are computed directly on the api every time 
that it’s queried. Information is pulled from different sources, including but
not limited to: airtable bases, hasura, etc. Each style record is returned as a 
dictionary containing its various prices within the onePrices key-value pair. 

The costs model/schema is available within the create-one-api Github repo linked
[here](https://github.com/resonance/create-one-api/blob/master/graph-api/source/graphql/models/style/style.graphql).

## About the Graph
The Graph works by querying the various sources within which Resonance stores 
data after being reached via an AWS gateway layer. That gateway layer can get
overloaded and is prone to timeouts both due to its own structural limitations
and due to the data sources the Graph queries, such as Airtable. 

## App Steps
The app is made up of steps, which currently are run in parallel with one 
another. Future iterations of the app may need to run steps sequentially in 
order to avoid trying to load too much data into snowflake at the same time. 

### Style Size Costs
Sync are either full or incremental. Incremental syncs filter data to only 
include styles that have been updated since the time of the last sync. Full 
syncs do not filter based on update time. There are basically four steps that 
the app takes to retrieve cost data: 

1. Preparation. This is the initial step of loading environmental variables and
such -- checking to see how the app needs to be run according to possible 
inputs. This is also where credentials for the Graph and Snowflake are retrieved 
from AWS Secrets Manager. 
2. Get Costs from Graph. This is a function which uses a single query to 
retrieve all available costs for each size of each style (i.e. all SKUs). The 
pricing components for each are retrieved if they are available. Cost info is 
only complete after the style has gone through the apply color queue for the 
first rendition of the style (i.e. after the style has been onboarded). 
Retrieved costs are filtered for styles that have been onboarded using the 
`isStyle3DOnboarded` field.
3. Flatten. This involves massaging the data retrieved from the Graph into a 
cleaner format -- namely a list of dicts. This format is easily convertible into 
a Pandas dataframe which can be easily loaded into Snowflake and modified using 
list/dict comprehension. A primary key for the records is also constructed here.
4. Send to Snowflake. The list of dicts is converted into a Pandas dataframe and 
duplicates are dropped. A stage table is created using the retrieved data. Data 
is merged from the stage table to the target, final table in the app 
environment's corresponding RAW database using an "upsert" strategy. When a 
matching record is found in the target table, the stage table record is ignored. 
When a stage table record's primary key is not found in the target table it is 
added to the target table.  

### Style Trims
This step retrieves data on trims from the Graph API. Each style can have a 
variety of trim (buttons, labels, etc) added to them and saved as a component of 
the style. These are stored within the style's bill of materials and have trim
specific fields and IDs to denote their purpose and use. Broader structure is 
relatively similar to `Style Size Costs` in terms of internal steps. 

Trims are currently stored in a variety of places within Airtable, most notably:
- Within res.Meta.ONE:
    - [Bill of Materials](https://airtable.com/appa7Sw0ML47cA8D1/tblnnt3vhPmPuBxAF)
    - [Trim](https://airtable.com/appa7Sw0ML47cA8D1/tbl3xL5qpJvsjROMs)
    - [Trim Taxonomy](https://airtable.com/appa7Sw0ML47cA8D1/tblMheiAcYd8U42YD)
- Within res.Magic.Purchasing:
    - [Materials table](https://airtable.com/appoaCebaYWsdqB30/tblJOLE1yur3YhF0K). 
    Within the Graph API schema, a trimItem "id" field 
    corresponds to an Airtable record ID to this table
- Within res.Trims
    - Most all tables, though data from the graph API is best linked to this 
    data via synced tables within the base
- Postgres. Our PostgreSQL database is in the process of having trims tables 
migrated as of Feb 13 2024

There are a variety of ID fields within the graphql query for this step. Check 
comments before the query itself for details on the schema

## Notes
The Graph API is very nebulous and some parts of it lack the greatest 
documentation. 
