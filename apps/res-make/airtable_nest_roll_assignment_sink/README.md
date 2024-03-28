# Airtable Nest-Roll Assignments Sink

Process that handles nest-roll assignment creation in Airtable.

When it receives Kafka messages from the Optimus nest-roll assignment job, it:
- adds the link between nests and rolls in Airtable
- inserts new print files records in Airtable
- starts a job which actually creates the printfiles

Times rob edited this file to trigger a redeploy = 3
