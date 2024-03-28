FROM python:3.8

RUN apt-get update && apt-get install -y bash gcc librdkafka-dev python3-dev musl-dev
RUN mkdir /app
WORKDIR /app
# Add requirements first so we don't have to reinstall each build
ADD ./apps/res-sell/product-tags/requirements.txt /app/
RUN pip install -r requirements.txt
RUN apt-get install ffmpeg libsm6 libxext6  -y

# Add in app code and libraries
RUN mkdir /app/res
ADD ./res /app/res
ADD ./apps/res-sell/product-tags/ /app/

# Set python path to discover files
ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENV PYTHONUNBUFFERED=0
ENV IGNORE_VIPS_LOAD_ERROR=1

EXPOSE 5000
CMD ["python", "/app/colorTags.py"]