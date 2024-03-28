from datetime import date, datetime

with open(
    "/Users/joelmorrobel/Documents/development/res-data-platform/apps/res-premises/test-app/5.txt",
    "a",
) as f:
    f.write(str(datetime.now()) + "\n")
    f.close
