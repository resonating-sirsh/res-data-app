
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-ray:latest

CMD ["python", "query_server/service.py"]

#docker build --pull --rm -f "Dockerfile" -t res-ray:query-service --platform linux/amd64 ../../../
#docker tag res-ray:query-service 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-ray:query-service & docker push 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-ray:query-service

#ray serve --deploy-docker 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-ray:query-service query_server/service.py