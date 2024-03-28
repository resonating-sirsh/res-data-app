This data collector is a quick-fix to get shopify data in IAC working again.

It is intended to be replaced by the batch_collector + webhooks at some point in the future.

Process:

1. pulls down orders from Shopify, either backfilled or for last X days (see info in Argo for more)
2. writes to S3
3. Uploads to temp table in Snowflake
4. merges orders + line items into "permanent" tables