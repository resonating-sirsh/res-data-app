FROM python:3.10
RUN apt-get update && apt-get install -y bash gcc python3-dev musl-dev 
RUN  pip install torch transformers[torch] accelerate datasets numpy Pillow scikit-learn tensorboard boto3 s3fs evaluate
ADD ./apps/res-ai-training/vit-trainer-a/ /app/
ADD ./res/connectors/ /app/res/connectors/
#ADD ./res/utils/ /app/res/utils/
ADD ./res/learn/datasets/S3ImageDataset/ /app/S3ImageDataset/ 
ENV PYTHONPATH "${PYTHONPATH}:/app"
CMD ["python", "/app/main.py"]