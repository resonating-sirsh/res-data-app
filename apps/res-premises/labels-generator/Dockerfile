ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest


ADD ./apps/res-premises/labels-generator/ /app/

# Set python path to discover files
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV PYTHONUNBUFFERED=0

RUN pip install blabel

ENTRYPOINT ["python", "/app/handle_event.py"]