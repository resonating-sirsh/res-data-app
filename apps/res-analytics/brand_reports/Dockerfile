ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest

WORKDIR /app/ 
RUN pip install --no-build-isolation lookml
RUN pip install --no-cache-dir --upgrade looker_sdk

ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=0

ENTRYPOINT ["python", "-m", "res",  "flow", "run", "infra.reports.looker_brand_reports"]
