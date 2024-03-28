ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

ADD ./apps/res-infrastructure/ask_one_service/ /app/
RUN pip install -r requirements.txt

# Set python path to discover files
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV PYTHONUNBUFFERED=0
ENTRYPOINT ["python", "/app/main.py"]
