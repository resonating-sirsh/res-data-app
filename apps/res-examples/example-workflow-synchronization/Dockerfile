ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest
WORKDIR /app/
COPY res /app/res/
ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=0

ENTRYPOINT ["python", "-m", "res",  "flow", "run", "flow_node_example"]
