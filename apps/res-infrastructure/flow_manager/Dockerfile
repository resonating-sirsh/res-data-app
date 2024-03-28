
ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}
WORKDIR /app

#install ap schduler to manage scheduler tasks
RUN pip install apscheduler

ADD ./apps/res-infrastructure/flow_manager/src/* /app/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 5000
CMD ["python", "/app/main.py"]
