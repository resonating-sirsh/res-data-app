ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-finance/quickbooks_invoice_generator/ /app/
RUN pip install -r requirements.txt
RUN pip install --upgrade snowflake-connector-python
ENV RES_TEAM "iamcurious"
ENV PYTHONPATH "${PYTHONPATH}:/app/"
