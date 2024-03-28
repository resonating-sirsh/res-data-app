# Klaviyo Data Integration

## Overview
This app contains code for for retrieving data from the [Klaviyo API](https://developers.klaviyo.com/en/reference/api_overview).
This API is accessible using a API token generated at the account level. 
Resonance has an overall account structure such that each individual brand
has an account beneath the Resonance main / umbrella account. Each API token
has its own corresponding data and discrete rate limit.

## App Structure
This app runs multiple steps with each step typically targeting a single API 
endpoint for all available accounts. These steps are broken out principally in 
order to avoid hitting rate limits but also to make the specific actions of each 
step more clear within the code. Functions within `helper_functions.py` are used 
to query a given API endpoint, retrieve data, and parse that data while 
uploading to Snowflake. Each step passes the name of its respective API endpoint
as well as a params dictionary containing details about the data being requested
such as a time range to filter by. 

Retrieved API data has two components, (1) the endpoint data itself (e.g. 
campaign data for the campaigns endpoint) and (2) included records related to
the endpoint data. Klaviyo’s newer APIs offer functionality via the 
relationships object which contains resources related to the resources requested 
via the API endpoint. This allows retrieval of, for example, the messages send
by a campaign when requesting info on that campaign, reducing the need for 
additional API calls to map these relationships out.

Included records must have their desired fields specified within URL parameters.
This is because the API does not automatically return all fields for related 
resources.

## Invocation and Parameters

The app is invoked using arguments that invoke various types of 
syncs. Syncs are one of:

* Incremental - The function retrieves the timestamp of the latest record of a 
step's target table from Snowflake and syncs forward
* Full - The function retrieves all available data for all account 
credentials stores in S3
* Unspecified - The function defaults to an incremental or full sync (depending 
on the step)

Data is retrieved from the API and upserted a table within the Snowflake RAW 
database under the KLAVIYO schema. By default the app is set to run 
incrementally at 5am and 5pm UTC daily. The `sync_type` parameter can be 
overridden by a developer in the Argo UI to perform full syncs. A single account
name can also be provided to run the app only for the account name provided.
