# res-data-platform

Welcome to the data platform repository which exists to hold code for data processing, workflow orchestration and (micro)services or what we just call "apps".

`res` is a library that contains many utils and connectors and a way to write "flows". Flows are really just python modules/functions for processing data but they come with some conventions and tools to make data processing code more consistent. If you are not writing a util or a connector, you are probably adding your code into a directory like `res/flows/make` or `res/flows/sell`.

You can also create **apps** that may or may not use the res library. Apps are just deployments that run on Kubernetes(Argo) It is recommended that you _do_ make use of the res library to build apps but you can bring your own docker images and do whatever you like to create apps.

Apps come in different flavours and there are examples for creating all of these in the [apps/examples](https://github.com/resonance/res-data-platform/tree/main/apps/res-examples) folder such as;

- **Long running services**: a long running service is something that is always running! For example, if you are processing events on a Kafka stream you can create a long-running service
- **res workflows**: if you want to create a job that runs once off in response to a schedule or an event such as a POST request you can create one of these. You can use the same template type to create either of these.
  - **cron jobs**: Uses argo cronjobs to schedule and run your workflow. You should add a cron configuration to the values-development/values-production yaml files as per the examples
  - **callable**: This is just like creating a lambda function. You should handle some sort of event payload and this payload will be passed to your template when clients post to res-connect/your-template

## Installation
### Requirements
- postgreSQL installed
- python3.10

### Installation
1. Clone the repo
2. You can `pip install -r res/docker/res-data/requirements.txt` or build from more specific requirements sets using pip compile (pip-tools) and the `.in` in that same folder. (In theory the .txt should have been compiled correctly but it may be easier to compile again for your setup)
3. Install direnv (e.g. with brew). Note res-data has an `.envrc` in its root that contains important environment variables. You should add have location of the res-data-platform repo to your `PYTHONPATH` at a minimum.
4. Check that `res` is "installed" by running the library as a module (you should see some help output): `python -m res`. This is just a sanity check to make sure its on your path.

Now that the library basically runs locally, the real fun begins when interacting with Kubernetes...

## Kubernetes and Argo

Everything in res-data platform runs on Argo/Kubernetes. You should check out the guides to make sure your environment is set up for these.

1. [Getting started with Kubernetes](https://coda.io/d/Platform-Documentation_dbtYplzht1S/Getting-Started_sumWZ#_lu2Up) (installing kubectl, connecting to the cluster, setting up kube-forwarder)
2. [Getting started with Argo](https://coda.io/d/Platform-Documentation_dbtYplzht1S/Argo-workflows_suZBL#_luaJ1) (installing the client and connecting to argo-server using kube-forwarder)

**Note**: Check with an admin to make sure you are added to the user list on the cluster(s)

## Deployment with Argo-CD

Argo-CD is separate to Argo workflows. When you open a PR with changes in res-data, there are build processes that do a few things

1. If you make changes to any files under the library `res/` then it will build res-data docker image and also deploy any res_data_workflows (Argo templates) pinned to the docker tag for the commit. (Note that res_data_workflows are auto-synced in production but in development)
2. If you make changes to any files under `apps/` it will be build the changed app and send it argo-cd to be "synced" (see below)

When you open a PR this causes Argo _on the development cluster_ to notice that you have created a new version of some code and it will indicate that the "app" is **out of sync**. We could auto sync this, which would push your new changes to the cluster. But we prefer to have the developer do this manually. As such, testing things on devs means...

1. Open a PR and waiting for your build action to complete successfully
2. Press "sync" on [argo-cd development](https://argocddev.resmagic.io/). You will notice the yellow out-of-sync status in the argo-cd client which you can connect to when you have open your ports in kube-forwarder

When you wish to push to production, you must have someone review your PR, then merge to main and then **sync on the production instance of argo**.

1. Merge reviewed PR to master
2. Press "sync" on [argo-cd production](https://argocd.resmagic.io/). Note that in some case sync is automatic:

       - Syncing on production is not required `/res_data_workflows` but is required in dev. 
       - `/apps` _may_ have auto-sync enabled on them as an option and will be auto-synced in dev _and_ prod.

### Notes

Switch to the **dev** k8s cluster

```
aws eks --region us-east-1 update-kubeconfig --name res-primary-cluster-dev
```

Switch to the **prod** k8s cluster

```
aws eks --region us-east-1 update-kubeconfig --name res-primary-cluster-prod
```

# Apis

There are currently four primary APIs (FastAPI) on the platform for corner stones of the platform

1. The Meta-ONE API ([Docs](https://data.resmagic.io/meta-one/docs)) - [Code](https://github.com/resonance/res-data-platform/tree/main/apps/res-meta/meta_one)
2. The Make-ONE API ([Docs](https://data.resmagic.io/make-one/docs)) - [Code](https://github.com/resonance/res-data-platform/tree/main/apps/res-make/make_one)
3. The Fulfillments API ([Docs](https://data.resmagic.io/fulfillment-one/docs)) - [Code](https://github.com/resonance/res-data-platform/tree/main/apps/res-fulfillment/fulfillment_one)
4. The Payments API ([Docs](https://data.resmagic.io/payments-api/docs)) - [Code](https://github.com/resonance/res-data-platform/tree/main/apps/res-finance/payments_api)

---

## requirements notes

when installing first time we get hit with these first for basic import (this should not happen if pip compile is used in a requirements.in file that contains each of these as per the docker/res-data folder)
```typing_extensions, tenacity, orjson, sentry_sdk, structlog,orjson,boto3,requests,pytz, numpy, pandas```

wrapt,compose,pydantic,stringcase, dnspython, byson,pymango,s3fs,dateparser,airtable,dataclasses_avroschema,dataclasses_jsonschema, opencv-python, scikit-image, gql==3.0.0a6, shapely==1.8.4

Pillow,  scikit-learn, redis, segno, qrcode, tinycss, ezdxf, aiohttp==3.7.4.post0, confluent-kafka[avro]==1.4.0, websockets

 ...
