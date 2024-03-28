ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

#this needed flast_cors but was not in the lite??

RUN pip install \
    "stringcase" \
    "flask_cors" \
    "fastapi-cognito==2.3.0" 

ADD ./apps/res-sell/shopify-webhooks/ /app/
COPY ./res-schemas /app/res-schemas/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/


ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV RES_TEAM "iamcurious"
EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "wsgi:app"]