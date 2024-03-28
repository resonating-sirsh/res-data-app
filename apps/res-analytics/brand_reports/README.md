# Brand Reports

This app generates Looker dashboard PDFs for each active brand,
saves to S3 (for access via create.ONE) and emails to brand users. Additional 
emails which will receive all reports are defined within the script itself at 
`res\flows\infra\reports\looker_brand_reports.py`.

Run with an empty event `{}`, 
or use one or more of the following settings:

```
{
    "send_email": "false",
    "brand_code": "TT",
    "dashboard_id": "403",
    "update_create_one": "true"
}
```

`send_email`: determines if we send emails, defaults to true. In development, 
even if send_email is true this will only send to internal resonance emails 
(not to brand users).

`brand_code`: set this to only run for a single brand.

`dashboard_id`: ID from Looker for the dashboard. Check the code to see options 
for dashboards.

`update_create_one`: A PDF is always generated and added to S3 and registered in 
Mongo. Create.ONE uses the column "Last Report File Id" from the Airtable 
res.Meta base > Subscribers table to determine which Mongo File ID to use for 
display in create.ONE...  in production, create.ONE is always updated, 
but you can use this setting to force development to update create.ONE for 
testing.
