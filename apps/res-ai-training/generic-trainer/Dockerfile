FROM python:3.10
RUN apt-get update && apt-get install -y bash gcc python3-dev musl-dev 
RUN  pip install torch transformers[torch] accelerate datasets numpy Pillow scikit-learn tensorboard boto3 s3fs evaluate torchvision
ADD ./apps/res-ai-training/generic-trainer/ /app/
ADD ./res/connectors/ /app/res/connectors/
#ADD ./res/utils/ /app/res/utils/
ADD ./res/learn/datasets/S3ImageDataset/ /app/S3ImageDataset/ 
ENV PYTHONPATH "${PYTHONPATH}:/app"
CMD ["python", "/app/main.py"]
#docker build --pull --rm -f "Dockerfile" -t res-train-seg:latest --platform linux/amd64 ../../../
#docker tag res-train-seg:latest 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-train-seg:latest