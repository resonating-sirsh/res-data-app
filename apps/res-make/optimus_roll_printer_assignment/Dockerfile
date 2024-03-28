ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

RUN mkdir -p /app
ADD ./apps/res-make/optimus_roll_printer_assignment/requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

ADD ./apps/res-make/optimus_roll_printer_assignment/ /app/
ENV RES_TEAM="optimus"

ENV PYTHONPATH "${PYTHONPATH}:/app/"
