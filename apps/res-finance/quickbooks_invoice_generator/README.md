### Invoice Generator

This will generate PDF invoices for brands each week, as well as a CSV report with detailed data.

## Generating Reports
It will automatically generate the reports each week, but you can run ad-hoc in Argo via the following link:
http://localhost:8004/workflow-templates/argo/invoice-generator-workflow

Make sure to enter a brand code if you want a specific brand, and start/end dates (it is inclusive of start date, and exclusive of end date)

Currently PDFs are uploaded to S3, at s3://iamcurious/invoices/{environment}/ . To deliver these to finance, you need to copy the files to the Box folder here:
https://resonancenyc.app.box.com/folder/143847228389?s=4b410aellcz6pwy0eseltplbzyixj7cc

Then notify the team in Slack via #brand_invoices channel...