# Mailchimp Data Integration

## Overview
This app contains code for for retrieving data from Mailchimp. Resonance partner 
brands have migrated over to using Klaviyo for email marketing -- as a result
accounts with Mailchimp will be cancelled. This app retrieves data from 
Mailchimp for storage in Snowflake.

## App Structure
This app uses multiple steps. These steps are broken out principally in order to 
avoid hitting rate limits but also to make the specific actions of each step 
more clear within the code. Functions within `helper_functions.py` are used to 
upsert (insert and update) data within Snowflake. Each step targets a particular 
section of Mailchimp account data. 

Data retrieval steps include:
* Campaigns - Campaigns are how emails are sent to a Mailchimp audience / user 
list
* Lists/Audiences - Lists/Audiences are where contacts are stored 
* Campaign Stats - Campaign level performance and marketing metrics
* Campaign Email Activity - The opens, clicks, etc for each subscriber of the 
list(s) a campaign was sent to
* Templates - A template is an HTML file used to create the layout and basic 
design for a campaign
* Stored Files - Data describing stored files (typically images)

The final two steps also captures files from an account file manager and stores 
them in an S3 bucket. This is in part to safeguard these images from deletion 
post account closure. It also allows us to create links to these images for 
viewing in Looker.


## Invocation and Parameters

The app is invoked using arguments passed to the command line. Arguments are:
1. dev_override - When set to "true" this allows the app to run in dev 
environments
2. api_key_override - When provided this replaces API keys retrieved from AWS 
secrets manager
3. server_override - When provided this replaces server names retrieved from AWS 
secrets manager
4. is_test - Whether the sync is test sync. Test syncs retrieve 1000 records for 
each account 
5. excluded_steps - Comma separated string of steps (python file names) to 
not run when the Argo template is submitted

There is no cron file for this job; ideally it is run once and only run 
subsequently to ingest data from future brands who join Resonance and migrate 
away from Mailchimp. The app is also built to add missing columns whenever new 
data arrives from an API call.  
