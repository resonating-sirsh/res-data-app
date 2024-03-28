# Zucc Meta Data Integration

## Overview
This app contains code for for retrieving data from the [Meta Marketing API](https://developers.facebook.com/docs/marketing-apis/overview/).
This API is accessible using a custom application which is then granted 
authorization to act as a system user for multiple Meta advertising accounts.
Each authorization instance has its own access token which is used to access
data from the corresponding account.

The app is named Zucc in order to prevent potential confusion from using the
name Meta, which generally is used within Resonance to refer to a base in
Airtable. Zucc is an alternate spelling of Zuck, a shortening of Zuckerberg 
(“Mark Zuckerberg”). 

## App Structure
This app uses a DAG structure to run multiple steps. These steps are broken out 
principally in order to avoid hitting rate limits but also to make the
specific actions of each step more clear within the code. Functions within 
`helper_functions.py` are used to query a given API endpoint, retrieve data, 
and parse that data while uploading to Snowflake. In order to keep this function
modular for the unique API calls of multiple steps, a function wrapping an API
call is an argument for the function. This allows each step to pass their own
calls without reproducing code unnecessarily. 

These API calls occur by calling a function from a pre-defined class within the
Facebook Business API. Classes refer to either nodes or edges within the 
Facebook graph api and contain functions to call edges deeper in the graph than
the object the class represents. Credentials are stored within S3 for each ad
account we report on.

Data within Meta Ads accounts exist in six levels:
* Ad Account - the highest level. Typically there is one ad account per Resonance 
brand
* Campaign - An individual ad campaign typically for a specific business goal
* Ad Set - Groups of ads that share the same daily or lifetime budget, schedule, 
bid type, bid info, and targeting data
* Ad - An individual advertisement object that is shown to customers online. Ads 
contain information to display an ad and associate it with an ad set. Each ad is 
associated with an ad set and all ads in a set have the same daily or lifetime 
budget, schedule, and targeting
* Creative - This level is not visible in the Meta Ad Manager API. It refers to 
the specific content for an ad, including images, videos and so on, that are 
part of each ad. 
* Images - An image used in a given ad creative

This app currently doesn't ingest Creative. This is for practicality reasons -- 
the API endpoint that retrieves all ad creatives for an account only allows 
retrieval of 50,000 ad creatives with no parameter options for filtering. 
Given we (already) have 20,000 ads in just the Tucker account, 
with each having multiple creatives, there is no way to simply get all 
creatives. If there is a desire and or need to ingest ad creatives, 
the most practical way to do it would be to add them as an `edge` query from 
the `ads` endpoint, which should only return the Ad Creative objects associated 
with the filterable data from the `ads` api endpoint. 

## Invocation and Parameters

The app is invoked using arguments that invoke various types of 
syncs. Syncs are one of:

* Incremental - The function retrieves the timestamp of the latest record of a 
step's target table from Snowflake and syncs forward. Not all steps perform 
incremental syncs because the data they target is always rather small
* Full - The function retrieves all available data for all ad accounts 
credentials stores in S3
* Unspecified - The function defaults to an incremental or full sync

Not all endpoints allow for incremental syncs and will just perform full syncs.

Data is retrieved from the API and upserted a table within the Snowflake RAW 
database under the ZUCC_META schema. By default the app is set to run 
incrementally at 4am and 4pm UTC daily. The `sync_type` parameter can be 
overridden by a developer in the Argo UI to perform full syncs. 
