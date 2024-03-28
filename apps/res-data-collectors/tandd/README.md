This Data collector gathers readings from T&D's Web Storage service and pushes the readings as an individual event to kafka. The sensors/loggers that will be retrieved are the ones marked as active on the following table:
https://airtable.com/tblxlzYVYNlWkzNF0/viwTaSRPvTIwysp8q?blocks=hide

This collector does the following checks:
- The time_diff field is -240 or UTC-4:00. Raises an Error.
- If logger has not reported data for the last two hours. Raises an Error.