ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-data-collectors/mailchimp_data_integration/ /app/

RUN pip install -r /app/requirements.txt

ENV RES_TEAM "iamcurious"
ENV PYTHONPATH "${PYTHONPATH}:/app/"
