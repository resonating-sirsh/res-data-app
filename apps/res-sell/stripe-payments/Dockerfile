ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-sell/stripe-payments/ /app/
RUN pip install -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app/"
EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--timeout", "600", "wsgi:app"]