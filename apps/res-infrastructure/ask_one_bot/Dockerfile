ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

WORKDIR /app

ADD ./apps/res-infrastructure/ask_one_bot/ /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

#for this thing we need both of these
RUN apt-get install poppler-utils
#added requirements for the app, beyond what we have on the res data image
RUN pip install pdf2image

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 5000
CMD ["python", "/app/main.py"]
