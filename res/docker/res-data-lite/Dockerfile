# This docker image can be used for most "apps" in the /apps folder
# It contains all of the res core libraries, and installs requirements for them

FROM python:3.10

# Install system libraries
RUN apt-get update && apt-get install -y bash gcc librdkafka-dev python3-dev musl-dev

# All app code goes in this folder
RUN mkdir /app
WORKDIR /app

# Copy in res libraries and install requirements
RUN mkdir /app/res
ADD ./res /app/res
ADD ./res/docker/res-data-lite/requirements.txt /app/res/
RUN pip install -r res/requirements.txt

# Set path for python module discovery, and set buffering so logs/print is viewable
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV PYTHONUNBUFFERED=0
