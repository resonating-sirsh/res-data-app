import requests

url = "https://datadev.resmagic.io/kgateway/submitevent"

payload = {
    "version": "1.0.0",
    "process_name": "kafka_to_airtable_testing",
    "topic": "res-infrastructure.kafka_to_airtable.airtable_updates",
    "data": {
        "submitting_app": "kafka_to_airtable_testing",
        "submitting_app_namespace": "res-infrastructure",
        "airtable_base": "appUjNEiW5uZDjCdI",
        "airtable_table": "tblAiAn1GXmYoom1I",
        "airtable_column": "Status",
        "airtable_record": "recboAnjBfBC7XBbi",
        "new_value": "Tod",
    },
}
response = requests.post(url, json=payload)
print(response.text)
