"""
The Trainer code can run locally or be deployed via Dockerfile to an Argoworkflow. 
An example dockerfile contents is shown here - package as required


MIGHT BE AN IDEA TO KEEP THEN IN RES DATA AND THE COPY THE BUILD CONTEXT
- COPY MODELS
- COPY DATASETS
- main.py imports and executes


##./Dockerfile 
##build and push this to ECR for use in the argo workflow running on EKS
##this captures the requirements but any dockerfile that copies main.py makes sense for this simple example
#
    FROM python:3.10
    RUN apt-get update && apt-get install -y bash gcc python3-dev musl-dev 
    RUN  pip install torch transformers[torch] accelerate datasets numpy Pillow scikit-learn tensorboard boto3
    ADD ./apps/res-ai-training/vit-trainer-a/ /app/
    CMD ["python", "/app/main.py"]
#
###

"""
from transformers import TrainingArguments
from transformers import Trainer
from transformers import ViTForImageClassification
import numpy as np
from datasets import load_metric
import torch
from transformers import ViTFeatureExtractor
from datasets import load_dataset
from torch import cuda

# ViTImageProcessor


def get_prepared_dataset_for_model(
    data_set_name,
    data_set_qualifier=None,
    model_name_or_path="google/vit-base-patch16-224-in21k",
):
    ds = (
        load_dataset("beans")
        if not data_set_qualifier
        else load_dataset(data_set_name, data_set_qualifier, trust_remote_code=True)
    )
    transform = transform_for_model(model_name_or_path)
    return ds.with_transform(transform)


def process_example_for_model(model_name_or_path="google/vit-base-patch16-224-in21k"):
    feature_extractor = ViTFeatureExtractor.from_pretrained(model_name_or_path)

    def process_example(example):
        inputs = feature_extractor(example["image"], return_tensors="pt")
        inputs["labels"] = example["labels"]
        return inputs

    return process_example


def transform_for_model(model_name_or_path="google/vit-base-patch16-224-in21k"):
    feature_extractor = ViTFeatureExtractor.from_pretrained(model_name_or_path)

    def _transform(example_batch):
        # Take a list of PIL images and turn them to pixel values
        inputs = feature_extractor(
            [x for x in example_batch["image"]], return_tensors="pt"
        )

        # Don't forget to include the labels!
        inputs["labels"] = example_batch["labels"]
        return inputs

    return _transform


def get_trainer(
    prepared_ds,
    fp16=True,
    epochs=5,
    model_name_or_path="google/vit-base-patch16-224-in21k",
    out_dir="./vit-base-beans",
):
    """
    prepared_ds = get_prepared_dataset_for_model( 'beans')
    trainer = get_trainer(prepared_ds)
    trainer.train()
    """

    metric = load_metric("accuracy")

    def collate_fn(batch):
        return {
            "pixel_values": torch.stack([x["pixel_values"] for x in batch]),
            "labels": torch.tensor([x["labels"] for x in batch]),
        }

    def compute_metrics(p):
        return metric.compute(
            predictions=np.argmax(p.predictions, axis=1), references=p.label_ids
        )

    feature_extractor = ViTFeatureExtractor.from_pretrained(model_name_or_path)

    labels = prepared_ds["train"].features["labels"].names

    model = ViTForImageClassification.from_pretrained(
        model_name_or_path,
        num_labels=len(labels),
        id2label={str(i): c for i, c in enumerate(labels)},
        label2id={c: str(i) for i, c in enumerate(labels)},
    )

    training_args = TrainingArguments(
        output_dir=out_dir,
        per_device_train_batch_size=16,
        evaluation_strategy="steps",
        num_train_epochs=epochs,
        fp16=fp16,
        save_steps=100,
        eval_steps=100,
        logging_steps=10,
        learning_rate=2e-4,
        save_total_limit=2,
        remove_unused_columns=False,
        push_to_hub=False,
        report_to="tensorboard",
        load_best_model_at_end=True,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        data_collator=collate_fn,
        compute_metrics=compute_metrics,
        train_dataset=prepared_ds["train"],
        eval_dataset=prepared_ds["validation"],
        tokenizer=feature_extractor,
    )

    return trainer


def _train(trainer):
    train_results = trainer.train()

    trainer.save_model()
    trainer.log_metrics("train", train_results.metrics)
    trainer.save_metrics("train", train_results.metrics)
    trainer.save_state()

    print("DONE")

    # metrics = trainer.evaluate(prepared_ds["validation"])
    # trainer.log_metrics("eval", metrics)
    # trainer.save_metrics("eval", metrics)


def upload_file(source, bucket="res-data-platform"):
    print(f"Uploading {source}")
    import boto3
    import os

    client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )
    path = f"test-models-out/{source.split('/')[-1]}"
    print(f"Uploading {source} to s3://{bucket}/{path}")
    client.upload_file(source, bucket, path)


def tb():
    import subprocess

    print("launch tensor board sub process")
    p = subprocess.Popen(
        [
            "tensorboard",
            "--samples_per_plugin",
            "images=100",
            "--logdir",
            "out",
            "--bind_all",
        ],
        start_new_session=True,
    )

    print("spawned...")


if __name__ == "__main__":
    out_dir = "./out"

    tb()

    from glob import glob

    print(list(glob("/app/*")))
    print(list(glob("/app/S3ImageDataset/*")))

    prepared_ds = get_prepared_dataset_for_model(
        "/app/S3ImageDataset/", "tileable_data"
    )

    device = "cuda" if cuda.is_available() else "cpu"
    cuda.empty_cache()
    print(f"<<<<<<DEVICE: {device}>>>>>>")
    if device == "cuda":
        print(f"<<<<<<CUDA VERSION: {torch.version.cuda}>>>>>>")

    trainer = get_trainer(prepared_ds, epochs=200, fp16=True, out_dir=out_dir)
    print("<<<<<<TRAIN NOW>>>>>>")
    train_results = trainer.train()
    print("<<<<<<SAVE NOW>>>>>>")
    trainer.save_model()
    trainer.log_metrics("train", train_results.metrics)
    trainer.save_metrics("train", train_results.metrics)
    trainer.save_state()
    print("<<<<<<UPLOADING TO S3>>>>>>")
    upload_file(f"{out_dir}/pytorch_model.bin")
    upload_file(f"{out_dir}/config.json")
    print("<<<<<DONE>>>>>>")


"""
snippets for tiling an image and testing we can detect what it is - need this to be consistent with how we trained

#g = nested_shapes[0]

from PIL import Image
from res.media.images.outlines import place_artwork_in_shape, draw_outline
im = place_artwork_in_shape(g, 's3://resmagic/uploads/992dfc9a-5ef1-42bc-9310-cec424cb6b03.tif')
im=Image.fromarray(draw_outline(im))
im.thumbnail(size=(224,224))

test = im.convert('RGB')

x = max(test.size) + 10
width, height = im.size
# Create a new square background
new_img = Image.new('RGB', (x, x), (0,0,0))

# Calculate positioning for pasting the image in the center
x_offset = (x - width) // 2
y_offset = (x - height) // 2

# Paste the image onto the new background
new_img.paste(test, (x_offset, y_offset))
test = new_img
test = test.resize((224,224))
print(test.size)
test



from res.learn.models.trainers.VitTrainer import *
import json
import pandas as pd

import torchvision.transforms as T

#prepared_ds = get_prepared_dataset_for_model("/Users/sirsh/code/res/res-data-platform/res/learn/datasets/S3ImageDataset",  'color_piece_parts')


with open('/Users/sirsh/Downloads/parts_model/config.json', 'r') as f:
    config = json.loads(f.read())

model = ViTForImageClassification.from_pretrained(
     '/Users/sirsh/Downloads/parts_model',
     num_labels=len(config['id2label']),
     id2label=config['id2label'],
     label2id=config['label2id'],
 )

print(config['id2label'])

tim = T.PILToTensor()(test).unsqueeze(0)
y = model(tim)
#provides a class index which can be checked against labels
print(labels[y.logits.argmax()],'-',y)
rdf = pd.DataFrame(list(y.logits.detach().numpy()[0]),columns=['probs']).reset_index()
rdf['label'] = rdf['index'].map(lambda x : config['id2label'].get(str(x)))
rdf.sort_values('probs',ascending=False).set_index('label').drop('index',1).plot(kind='barh')




"""


"""

import os
os.environ['AWS_ACCOUNT_ID'] = '286292902993'
os.environ['WORKFLOW_PATH'] = '/Users/sirsh/code/res/res-cloud/bootstrap/eks-sketch'

from res.utils.dev import deploy_trainer
deploy_trainer('res-train', docker_dir='/Users/sirsh/code/res/res-data-platform/apps/res-ai-training/vit-trainer-a')


"""
