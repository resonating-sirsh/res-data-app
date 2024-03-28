# Stream processor

Using this as an example to illustrate some useful dev practices and some of the `res-data` stack. Take a look at the project structure as it is quite minimal and can be used as a template for any long running process. It calls out to one external function in `res-data` where the logic is and it uses `FlowContext` to access `connectors`. This is why this folder is quite minimal and can be used as a template. The same function can be run in a scheduled context by invoking the generic `res-connect/node/` endpoint as illustrated below. `FlowContext` helps with mocking connectors although this is a work in progress.  

The first thing we want to do is setup our python environment. `pip-tools` and `venv` are used to create an isolated environment. If the unit tests pass in this environment, then it is likely the app will run on docker and kubernetes. The secrete is loaded from secrets manager via `res.env` so there is no need to set up env variables which is one common source of deployment or integration issues that would appear outside of this context. 

To speed up docker builds, we base our image off of a pre-built one and copy `res-data` and our app onto it. In this case the base image is bigger than it needs to be but you could create your own lighter docker image. In other projects we will explore Docker builds. The build step is updated to cache docker builds so that the ARgo CD step is as quick as possible, simply copying the latest code and caching all pip installs etc.


## Setting up your env

The root of our app is `res-data-platform/apps/res-infrastructure/assembly-event-listener`. Our `PYTHONPATH` contains the `res` library. When you import `res` it loads `res.utils` which contains core/common features and the minimal requirements for running res are to install the requirements that are in `.requirements/core=requirements.in` If you do not know what a .in file, this is a "pre requirements" file that pip-compile will use to create out actual requirements file. Additionally within this project we have additional requirements that we will need just for this app. First lets create an environment. We can either create it in our current workspace or in the `res-data-platform/.direnv` for VSCode to pick it up. 

```python
#from res -data-platform run these commands
python -m venv .direnv/apps-assembly-el
source .direnv/apps-assembly-el/bin/activate
python -m pip install --upgrade pip
#this demonstrates we have an empty-ish env
pip list
```

now we are going to add a requirements file using pip compile. Change directory to `apps/res-infrastructure/assembly-event-listener`

```bash
pip-compile ../../../.requirements/core-requirements.in \
            requirements/dev.in \
            -o requirements.txt
```

Now add these to your virtual environment (make sure you are in your virtual env)

```bash
pip install -r requirements.txt
#afterward pip list
```

### Configuring visual studio 

Choose the python environment by clicking in the bottom panel where it says `Python 3.x` and check the list. If you have just created your virtual environment click the refresh button (or restart VSCode) - you should see the virtual env you created e.g. `apps-assembly-el`. Select it. 

Open a terminal and make sure you can import `res` - if you cannot you are missing core requirements.

#### configure and run tests
If you wanted to test `res` you could just configure `pytest` to use the test folder `res-data-platform/test` which is where all `res` tests live, mirroring the module structure of rest itself. Note `pytest` can be confused if it does not know the difference between say `res.utils` itself and `test/res.utils` so for this reason we always add an `__init__.py` in the test folder to make it a distinct module. 

- set up visual studio for your app


#### troubleshoot testing

Error -  complains that module not found: This is due to how pytest imports modules. It either imports a module if it can or adds things to syspath. If adding to syspath they have to be unique which is hard to guarantee when we have many test folder in the repo. If you add `__init__.py` in your modules and test folders everything can be treated as a python module but there is catch. You may have named your apps folder with something like `my-module-name` but because this is not a valid python module with underscores pytest will fail but the error messages can be confusing. So perhaps our best strategy is to use a convention to name top level app folders as valid python names e.g. `my_module_name`. Note, if this seems restrictive it is because we have multiple constraints we are trying to satisfy i.e. have multiple sub modules/apps and have pytest discover all tests in the tree. If for example we just had one test folder mirroring a typically python module, we would not have this problem. To understand about how pytest imports modules see [here](https://docs.pytest.org/en/6.2.x/pythonpath.html)

Error -  attempt to import outside top of module structure: In your tests you should use relative imports if your app is not in the `PYTHONPATH`. We put `res` in the `PYTHONPATH` so we can import `res.*`. However you should not put your app in the `PYTHONPATH` but instead develop from that directory. In this case your tests should import relative e.g. `..src` but to do this you need to make sure your app is a module by adding a `__init__.py` in the root of the app. Just to be clear, each app is its own module that is not in the PYTHONPATH but developed in isolation. And it should be isolated!


## Different kinds of deployments

We try to make each type of deployment as similar as possible and we do this by keeping as much code in the core library as possible and then only add service-specific pieces in the App. I can run this current app as a long-running service e.g. as a kafka relay (consumer-producer) or i can run it as a cronjob on argo. We could also call our code in response to a REST call. In each case we should require minimal code in the actual deployment specific to either of these things. A long running service simply needs a docker image and a helm chart with a main function that calls a function in `res`. The Argo cronjob or REST call is similar. We go once step further in the later cases so that we do not even need to deploy anything but instead use the FlowAPI. TODO add link. The FlowAPI allows to post to res-connect e.g. `res-connect/flows/my-service-flow-name/` to trigger a flow. Under the hood this uses a generic Argo template to run res on a docker image and invoke your function. Simple. The only thing you need to worry about are the args and the context with information such as special memory requirements or a docker image Tag/ID. Similarly, the CRON job will work in exactly the same way; and ARGO cronjob will call the generic node with the same payload that is used over REST. With this in mind, lets setup our actual logic, unit test it, commit it and invoke it in these ways. When we have done that, we will also setup a long-running service option. In this APPs repo, the only code we will need is the "long running service" code and/or a cron schedule. Additionally your particular service context's requirements/docker can be managed here if they are different to the `res` default. Take note of this as managing deployed docker contexts is one of the main book-keeping challenges. 

### Creating and testing functions in res



### Making REST calls to your (deployed) service

### Deploying a Cron job

### Deploying a long-running service


# Note on res library

The `res` library is a shared library so there is one standard build for this and it is not automatically triggered by git actions related to apps. There is a build action that builds and caches res docker images and tests the entire library. 

:space_invader: :space_invader:
