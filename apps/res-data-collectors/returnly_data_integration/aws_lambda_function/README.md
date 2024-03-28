# returnly-data-integration

## Overview
This project contains code for Lamdba functions using the Returnly API. These
are invoked using eventbridge schedules with events matching the type of sync
they invoke. Sync are either incremental, meaning they use the date of the last
record update in Snowflake as the lower bound for the API call, or full refresh,
meaning they retrieve all available data and upsert it into Snowflake.

The application uses several AWS resources, including Lambda functions and an 
API Gateway API. These resources are defined in the `template.yaml` file in this 
project. The template can be updated to add AWS resources through the same 
deployment process that updates the application code.

## Locally Invoke

Locally invoke incremental syncs:
```
sam build && sam local invoke --event events/incremental_sync.json
```

Locally invoke full refresh :
```
sam build && sam local invoke --event events/full refresh_sync.json
```

## Deploy

The Serverless Application Model Command Line Interface (SAM CLI) is an 
extension of the AWS CLI that adds functionality for building and testing Lambda 
applications. It uses Docker to run functions in an Amazon Linux environment 
that matches Lambda. It can also emulate this application's build environment 
and API.

To use the SAM CLI, the following tools are needed.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* [Python 3 installed](https://www.python.org/downloads/)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

Deploy using the standard SAM commands, required capabilities are listed in the 
samconfig.toml file. If using a source-code editor like VSCode these commands
must be entered within an integrated terminal for the directory this file is
contained within.

```
sam build && sam deploy --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM
```

The first command will build the source of your application. The second command will package and deploy your application to AWS, with a series of prompts:

* **Stack Name**: The name of the stack to deploy to CloudFormation. This should be unique to your account and region, and a good starting point would be something matching your project name.
* **AWS Region**: The AWS region you want to deploy your app to.
* **Confirm changes before deploy**: If set to yes, any change sets will be shown to you before execution for manual review. If set to no, the AWS SAM CLI will automatically deploy application changes.
* **Allow SAM CLI IAM role creation**: Many AWS SAM templates, including this example, create AWS IAM roles required for the AWS Lambda function(s) included to access AWS services. By default, these are scoped down to minimum required permissions. To deploy an AWS CloudFormation stack which creates or modifies IAM roles, the `CAPABILITY_IAM` value for `capabilities` must be provided. If permission isn't provided through this prompt, to deploy this example you must explicitly pass `--capabilities CAPABILITY_IAM` to the `sam deploy` command.
* **Save arguments to samconfig.toml**: If set to yes, your choices will be saved to a configuration file inside the project, so that in the future you can just re-run `sam deploy` without parameters to deploy changes to your application.

You can find your API Gateway Endpoint URL in the output values displayed after deployment.
