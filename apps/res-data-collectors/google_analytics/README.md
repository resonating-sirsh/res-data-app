### Google Analytics
This data collector pulls historic and current data from Google Analytics, and inserts into Snowflake.

To use this, go to http://localhost:8004/workflow-templates/argo/google-analytics and submit a new job. you can optionally set the "BACKFILL" flag to "TRUE", which will go back to 2020 and get old data. If you don't pass the flag, it will do nothing.
