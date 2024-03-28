# VStitcher Job Runner

## Overview

The VStitcher Job Runner is the platform companion to the [VStitcher Plugin](https://github.com/resonance/browzwear), which runs inside VStitcher itself. This runs on the same machine as VStitcher, and kicks off jobs and handles the upload and creation of `create.assets` to Hasura + s3.

The TLDR:
- <b>VStitcher Plugin</b>: very pure, few dependencies. Runs inside VStitcher itself
- <b>VStitcher Job Runner</b>: many platform dependencies, handles feeding jobs into VStitcher and handles the outputs


## Installation
- Setup Instructions are found in [Coda](https://coda.io/d/Platform-Documentation_dbtYplzht1S/VStitcher-Job-Runners-General_su9mx#_luGBO)

## About
#### Why two services?
Short answer, python doesn't do multi-threading. Not true multi-threading at least. So when the VStitcher jobs are kicked off via a python `cmd` they sometimes <b>block the python process entirely.</b> 

This is a problem, because we want to upload assets ASAP. The use case is that Brand Onboarding might want to generate 6 or 7 assets, one of which might take 20 minutes. We want to upload everything as soon as it's generated.

#### What's the flow?

1. The `job_runner` workers poll Hasura and claim a job.
2. The `job_runner` creates the working directory for the job, and downloads all necessary assets, including the `job.json` which will tell `vstitcher_plugin` what to do. (e.g. what commands to run)
3. `vstitcher_plugin` is invoked against the directory, and outputs assets like ray-traced renders, carousels, etc. into the job folder.
4. `file_sync_daemon` listens for file creation and updates. It waits for the files to be done writing, and when it's a filename we expect from the `job.json` it uploads the asset. Otherwise, the file is ignored.
    - When the file is named `.DONE`, the `job_runner` knows that the job is finished.  It deletes the directory, and marks the job as finished in Hasura.


#### What's in the `job.json`?

The `job.json` is a mix of the `platform.job_preset` and the `job_details`. It's everything required to run a job, including
- what steps/exports to run
- where to put those assets
- what artwork files to use?

The schema for both the preset and the job details are encoded in JSON Schema in the schemas directory.
