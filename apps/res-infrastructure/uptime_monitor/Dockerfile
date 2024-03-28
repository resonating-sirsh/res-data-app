ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-infrastructure/uptime_monitor/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
EXPOSE 5000
CMD ["python", "/app/src/main.py"]