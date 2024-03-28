SHELL=/bin/sh
current_dir=$(shell pwd)
user=$(shell whoami)
namespace=res-data-platform
release=$(namespace)
current_context=$(shell kubectl config current-context)
development_repo=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-k8s-development
development_cluster=arn:aws:eks:us-east-1:286292902993:cluster/res-primary-cluster-dev
is_development=$(shell [ $(current_context) == $(development_cluster) ] && echo 1 || echo 0)

help:  ##                       Show this help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

pods: ##                        Shows pods in this namespace
	kubectl get pods --namespace $(namespace)

logs: ## Show logs for a given app. Use app=abc after the make command to select
	kubectl logs $(shell echo $(shell kubectl get pods --namespace $(namespace) | grep $(app)) | cut -d " " -f 1) --namespace $(namespace) -f

shell: ## Opens an interactive shell on a pod for a given app. Use app=abc after the make command to select
	kubectl exec -it $(shell echo $(shell kubectl get pods --namespace $(namespace) | grep $(app)) | cut -d " " -f 1) --namespace $(namespace) -- bash

upgrade-local: ##               Builds local docker image and deploys to local cluster
	- kubectl create namespace $(namespace)
	helm upgrade --install $(release) --values kubernetes/values-local.yaml kubernetes --namespace $(namespace)

delete-local: ##                Deletes development version in local cluster
	helm delete $(release) --namespace $(namespace)

ecr-login: ##                   Logs into ECR to deploy containers
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com

build-development: ##           Builds docker image and pushes to ECR for development
	docker build -t $(development_repo):$(release)-$(app) ../ -f ../microservices/$(app)/Dockerfile
	docker push $(development_repo):$(release)-$(app)

build-development-m1: ##           Builds docker image and pushes to ECR for development, for Apple M1 Silicon
	docker buildx build --platform linux/amd64 --push -t $(development_repo):$(release)-$(app) ../ -f ../microservices/$(app)/Dockerfile
	# docker push $(development_repo):$(release)-$(app)

build-flyway-development-m1: ##           Builds docker image and pushes to ECR for development, for Apple M1 Silicon
	docker buildx build --build-arg SQL_FOLDER=../microservices/$(app)/migrations --platform linux/amd64 --push -t $(development_repo):$(release)-$(app)-flyway ../ -f postgraphile_backend/Dockerfile

upgrade-development-process: ##         Deploys to development kubernetes cluster
ifeq ($(is_development),1)
	- kubectl create namespace $(namespace)
	helm upgrade --install $(app) --values base_values_development.yaml --values res_long_running_process/values.yaml --values ../microservices/$(app)/values-development.yaml res_long_running_process --namespace $(namespace)
else
	@echo "Current context set to $(current_context). Please set to development"
endif

upgrade-development-service: ##         Deploys to development kubernetes cluster
ifeq ($(is_development),1)
	- kubectl create namespace $(namespace)
	helm upgrade --install $(app) --values base_values_development.yaml --values res_long_running_service/values.yaml --values ../microservices/$(app)/values-development.yaml res_long_running_service --namespace $(namespace)
else
	@echo "Current context set to $(current_context). Please set to development"
endif

upgrade-development-backend: ##         Deploys to development kubernetes cluster
ifeq ($(is_development),1)
	- kubectl create namespace $(namespace)
	helm upgrade --install $(app) --values base_values_development.yaml --values postgraphile_backend/values.yaml --values ../microservices/$(app)/values-development.yaml postgraphile_backend --namespace $(namespace)
else
	@echo "Current context set to $(current_context). Please set to development"
endif

delete-development: ##          Deletes development Helm release
	helm delete $(app) --namespace $(namespace)
