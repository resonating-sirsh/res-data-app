### Adwords Collector
Pulls Adwords reports for Resonance Brands, and loads directly into Snowflake.

#### Configuration
You can set the BACKFILL flag in Argo to "true" to run for the full history (past 4 years). If you don't run for the full history, this will run for the prior 2 days each night, with deduplication in Snowflake. 

All configuration for brands is stored in `main.py`. To add a new brand, you'll need the account ID and the brand code.

#### Credentials
Adwords has one set of credentials for Resonance, which should work for all brands.

The Google Adwords Client claims to expect a YAML file, however passing a JSON file appears to work.
We've used JSON, since it's more convenient to serialize. So to rotate the credentials, manually convert
the Adwords YAML into serialized JSON.
