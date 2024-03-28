ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest

ADD ./apps/res-platform/change-events-processor/ /app/
RUN pip install -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app/"
EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "wsgi:app", "--timeout", "120"]
