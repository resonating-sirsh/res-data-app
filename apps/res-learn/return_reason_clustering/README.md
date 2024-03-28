# Return Reason Clustering

## Overview

This app contains code for clustering reasons / comments given by customers 
returning garments. The business goal of this is to better understand patterns 
and feedback surrounding the quality of goods produced as well as the customer
experience in receiving those goods. Data on returns comes from Snowflake 
tables, where returns from different platforms Resonance has used over time (
e.g. Returnly, Aftership) are combined into a single table. When applicable, 
each line item of a return is connected with that line item's return reason 
notes / customer comment. These are distinct from the standardized return 
reasons that customers can choose from when initiating a return request. Only
freetext-style comments written by the customer are used in this app.  

Data is taken from Snowflake and stored within a LanceDB table in AWS S3, 
where the return reasons are embedded using APIs from OpenAI. Following that, an 
additional step of the app (to-be-added) clusters these return reason embeddings
and has an LLM label each cluster using a sample of the return notes within it. 

## Invocation and Parameters

The app is invoked using arguments that invoke various types of 
syncs. Syncs are one of:

* Incremental - The function retrieves the timestamp of the latest record of a 
step's target table from Snowflake and syncs records that are newer than that
timestamp
* Full - The function retrieves all available data from Snowflake and loads it 
into S3 before clustering
* Test - The function retrieves 100 rows of data from Snowflake

By default the app is set to run incrementally weekly. The `sync_type` parameter 
can be overridden by a developer in the Argo UI to perform full syncs.
