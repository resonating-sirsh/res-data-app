This folder contains code for the Resonance Core Libraries, along with application-specific code used in res-workflows

You can use these libraries by creating a new app in /apps, and in your Dockerfile use `FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-development:latest` or res-data-lite-production:latest for production.

The two Docker images are built by different github actions: res-data is built by res_data_workflows_build_test_deploy, and res-data-lite is built by apps_build_test_deploy

:space_invader:

hello :)
