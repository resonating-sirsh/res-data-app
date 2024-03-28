# exchange_rate_collector

## Overview
This app contains code for for retrieving historical data about currency 
exchange rates using the [Exchange Rates Data API](https://apilayer.com/marketplace/exchangerates_data-api#). 
These are invoked using environment variables that invoke various syncs. Syncs 
are one of:

* Incremental - The function retrieves the latest record from Snowflake and syncs forward
* Latest - The function retrieves the latest available rates
* Full - The function retrieves all available history since 2017
* Unspecified - The function defaults to a full, historical sync

Data is retrieved from the API and stored in a historical table within the 
Snowflake RAW database under the schema MACROECONOMIC_DATA.

## Notes / Caveats
Historical information for a given day is only available after a certain, 
unspecified point in the day. Additional, requests cannot involve invalid time 
ranges that either request future records or have start dates that are not 
before end dates. The function has conditional logic written to avoid calling
for records that do not or do not yet exist.

## Rate limits
The Exchange Rates Data API uses API keys to authenticate requests. This 
function uses a single free subscription which allows for up to 250 requests per 
month. Once rate limits are reached the function will begin to return error
messages. The function is set to retrieve full history once per month on the 1st
of the month, incremental history once per week on Sundays, and latest info 
twice a day at 9am EST and 9pm EST. This should amount to ~65 requests per month
and allows for additional manual syncs while still remaining far from the rate 
limit.  

All requests made to the API must hold a custom HTTP header named "apikey". All 
API requests must be made over HTTPS. Calls made over plain HTTP will fail. 
API requests without authentication will also fail.

Full information on rate limits is available [here](https://apilayer.com/marketplace/exchangerates_data-api#rate-limits).
