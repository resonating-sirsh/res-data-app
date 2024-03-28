## Airtable to Snowflake data collector

#### Overview
This process pulls tables down from Airtable to S3, loads into Snowflake, and builds a "latest version" view of each table for use in IAC.

#### Optional event settings
> {
>     "base": "appABCDE",
>     "tables": ["tblWXYZ","tblCVBN"],
>     "start_date": "2021-01-01",
>     "end_date": "2021-10-01"
> }

If no start/end date is included, this runs for the latest day. If dates are included, it will look for historical archives in S3, and load into Snowflake. (It isn't possible to pull historical data from Airtable, we can only pull snapshots for the current state)

If no tables are included, it will run all tables in the base. If no base is included it will include all bases specified in src/01_generate_tables.py

#### Notes

The final step is to run "patch" queries, to heal broken data. These will only run if no base/table is specified, or the base/table is one of the ones affected by the patch queries.

#### In case of errors

The main cause of errors is bad data coming from airtable -- this process uses the Airtable column
types to generate the Snowflake table. If somebody has changed the column type in Airtable and already entered old data, it can break. For example:

ColA is text, and someone adds in rows "x", "y", "z"
User changes ColA to an int the next week, deletes all data, then adds "1", "2", "3"
Now we have historical data with text and new data with int, so the load will break.

Solution is to add "ColA" to the "CORRUPTED_COLUMNS" dict in 04_create_columnar_tables.py
