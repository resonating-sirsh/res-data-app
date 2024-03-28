import requests

url = "https://datadev.resmagic.io/kgateway/submitevent"

# Open the table "Ben-Printjobdata Processing" in the DEVELOPMENT base in Airtable
# Add a random number into the "Key" column, and add that same number to the "Name" section in the payload below
# Also update the Status to be either 1,2 or other -- 1 will set to PRINTED, 2 to PROCESSING, and anything else will do nothing


payload = {
    "version": "1.0.0",
    "process_name": "print_job_data_processor_testing",
    "topic": "printJobData",
    "data": {
        "MSjobinfo": {
            "Printer": "yoda",
            "Jobid": "41",
            "Name": "12345",
            "Status": "2",
            "PrintMode": "C4: 16passes 8levels HiSpeed HQ",
            "Direction": "Uni",
            "Media": "coton effjyt 2.2m",
            "HeadGap": "1.2",
            "Width": "830",
            "RequestedLength": "941",
            "StartTime": "1463176497",
            "EndTime": "1463176507",
            "PrintedLength": "0",
            "FoldDetected": "0",
            "MSinkusage": {
                "Ink": [
                    {
                        "Color": "Cyan",
                        "Type": "[R] Huntsman Novacron XKS-HD",
                        "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                        "Usage": {"ToPrint": "0.000", "ToClean": "0.000"},
                    },
                    {
                        "Color": "Magenta",
                        "Type": "[R] Huntsman Novacron XKS-HD",
                        "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                        "Usage": {"ToPrint": "0.000", "ToClean": "0.000"},
                    },
                    {
                        "Color": "Yellow",
                        "Type": "[R] Huntsman Novacron XKS-HD",
                        "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                        "Usage": {"ToPrint": "0.000", "ToClean": "0.000"},
                    },
                    {
                        "Color": "Black",
                        "Type": "[R] Huntsman Novacron XKS",
                        "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                        "Usage": {"ToPrint": "0.000", "ToClean": "0.000"},
                    },
                    {
                        "Color": "Black",
                        "Type": "[A] Huntsman Lanaset XKS",
                        "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                        "Usage": {"ToPrint": "0.000", "ToClean": "0.000"},
                    },
                    {
                        "Color": "Orange",
                        "Type": "[R] Huntsman Novacron XKS-HD",
                        "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                        "Usage": {"ToPrint": "0.000", "ToClean": "0.000"},
                    },
                    {
                        "Color": "Blue",
                        "Type": "[R] Huntsman Novacron XKS-HD",
                        "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                        "Usage": {"ToPrint": "0.000", "ToClean": "0.000"},
                    },
                    {
                        "Color": "Transparent",
                        "Type": "[U] MS Universal",
                        "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                        "Usage": {"ToPrint": "0.000", "ToClean": "0.000"},
                    },
                ]
            },
        }
    },
}
response = requests.post(url, json=payload)
print(response.text)
