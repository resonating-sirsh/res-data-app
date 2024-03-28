# Shopify Data Integration

## Overview
This app contains code for for retrieving data from the [Shopify Admin REST API](https://shopify.dev/api/admin-rest) 
and [Shopify GraphQL Admin API](https://shopify.dev/api/admin-graphql).
This API is accessible using a custom application installed for each Shopify
shop for each active Resonance brand. These apps are granted read access to most
all Shopify data and each have a distinct API key for the Admin API. This app
aims to connect to the Admin API using each shop's custom app credentials and 
retrieve a variety of information from different endpoints. 

Endpoints currently with an ingestion step within this app:
* [Abandoned Checkouts](https://shopify.dev/api/admin-rest/2023-01/resources/abandoned-checkouts)
* [Carrier Services](https://shopify.dev/api/admin-rest/2023-01/resources/carrierservice)
* [Customer Visits](https://shopify.dev/api/admin-graphql/2023-01/objects/CustomerJourneySummary)
* [Customers](https://shopify.dev/api/admin-rest/2022-10/resources/customer)
* [Fulfillment Services](https://shopify.dev/api/admin-rest/2022-10/resources/fulfillmentservice)
* [Orders](https://shopify.dev/api/admin-rest/2022-10/resources/orders)
* [Product Performance](https://shopify.dev/api/shopifyql/datasets/products-dataset)
* [Products](https://shopify.dev/api/admin-rest/2022-10/resources/product)
* [Shop Content Events](https://shopify.dev/api/admin-rest/2022-10/resources/event)
* [Shops](https://shopify.dev/api/admin-rest/2023-01/resources/shop)

## App Structure
This app uses a DAG structure to run one step per bullet point above. These
steps are broken out principally in order to avoid hitting rate limits. The
Shopify Admin API sets rate limits per app and per shop. This means that each
shop is confined to a certain number of requests. In order to take advantage of
keep-alive connections to the API those requests are generally performed 
entirely for one endpoint before moving to the next.

Because the limits are for each individual store there is no rate-related
downside to performing requests for different shops at the same time. To take
advantage of this the app performs API calls asynchronously and significantly 
reduces runtime as a result. 

Information about rate limits is available [here](https://shopify.dev/api/usage/rate-limits).

The majority of steps make use of Shopify's REST Admin API. Product Performance
and Customer Visits make use of the GraphQL API do to their resources only being
available via that API. Customer Visits specifically makes use of a Shopify bulk
query feature that performs a similar asynchronous query operation but with the
rate-limiting and throttling handled by Shopify's servers rather than handled
client-side by the app. Both GraphQL steps use different functions than the
other steps to call their API(s).

## Invocation and Parameters

The app is invoked using environment variables that invoke various types of 
syncs. Syncs are one of:

* Incremental - The function retrieves the timestamp of the latest record of a 
step's target table from Snowflake and syncs forward
* Full - The function retrieves all available data for all shops
* Unspecified - The function defaults to a full sync

Data is retrieved from the API and upserted a table within the Snowflake RAW 
database under the SHOPIFY schema. By default the app is set to run 
incrementally at 4am and 4pm UTC daily. The `sync_type` parameter can be 
overridden by a developer in the Argo UI to perform full syncs. 

Additionally, you may provide the app with a shop domain name matching one of 
the names within the Shopify API credentials AWS secret to run the sync solely 
for the submitted shop.
