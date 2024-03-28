import requests

url = "https://datadev.resmagic.io/kgateway/submitevent"

payload = {
    "version": "1.0.0",
    "process_name": "kafka_to_airtable_testing",
    "topic": "res_infrastructure.kafka_to_airtable.airtable_multifields_update",
    "data": {
        "submitting_app": "kafka_to_airtable_testing",
        "submitting_app_namespace": "res-infrastructure",
        "airtable_base": "appyxEVba0zT38PP7",
        "airtable_table": "tbluZVKHJ2xDTF8sX",
        "airtable_record": "rec5KkwsJFLfjXc04",
        "payload": {"Brand": "Apple-test", "Name": "Joel-test"},
    },
}
response = requests.post(url, json=payload)
print(response.text)
