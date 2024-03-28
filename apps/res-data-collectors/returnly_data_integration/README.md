# returnly-data-integration

## Deprecation
Returnly was shut down completely as of October 1st 2023

## Overview
This project contains code using the Returnly API. These are invoked using cron 
jobs via argo. Sync are either incremental, meaning they use the date of the 
last record update in Snowflake as the lower bound for the API call, or full 
refresh, meaning they retrieve all available data and upsert it into Snowflake.
