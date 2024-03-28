# Airtable webhooks

A listener for receiving pings from Airtable Webhooks API and pull changes on tables.

- We maintain a list of fields (in base/table) to sync in a config manager and keep the Webhook spec up to date
- We pull changes and push them to a Kafka topic