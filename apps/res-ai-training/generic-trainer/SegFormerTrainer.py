"""

today move labels and stuff to the dataset and use throughout

"""

from transformers import SegformerForSemanticSegmentation
from transformers import TrainingArguments
import torch
from torch import nn
import evaluate
from transformers import Trainer
from torchvision.transforms import ColorJitter
from transformers import SegformerImageProcessor
import pandas as pd

jitter = ColorJitter(brightness=0.25, contrast=0.25, saturation=0.25, hue=0.1)


def get_transforms(processor):
    def train_transforms(example_batch):
        images = [jitter(x) for x in example_batch["pixel_values"]]
        labels = [x for x in example_batch["label"]]
        inputs = processor(images, labels)
        return inputs

    def val_transforms(example_batch):
        images = [x for x in example_batch["pixel_values"]]
        labels = [x for x in example_batch["label"]]
        inputs = processor(images, labels)
        return inputs

    return train_transforms, val_transforms


# for now - add this to the dataset
# label2id = {
#     "Background": 0,
#     "Self": 1,
#     "Bodice": 2,
#     "Lower Body": 3,
#     "Sleeve / Arm": 4,
#     "Neck": 5,
#     "Dependent Accessory": 6,
#     "Independent Accessory": 7,
# }

label2id = {
    "SLPNL": 1,
    "BKPNL": 2,
    "FTPNL": 3,
    "NKBND": 4,
    "BKYKE": 5,
    "FTFAC": 6,
    "FTPKT": 7,
    "SDGSS": 8,
    "BKLOP": 9,
    "NKCLR": 10,
    "FTPLK": 11,
    "BNFAC": 12,
    "FTPKB": 13,
    "BKPKT": 14,
    "WTWBN": 15,
    "SLCUF": 16,
    "FTFLE": 17,
    "FSPNL": 18,
    "SDPKB": 19,
    "SDFLP": 20,
    "SDPKT": 21,
    "SDPNL": 22,
    "BSPNL": 23,
    "FTYKE": 24,
    "STPNL": 25,
    "SUPNL": 26,
    "WTTAB": 27,
    "FTFLP": 28,
    "BYPNL": 29,
    "NKCLS": 30,
    "SLPLK": 31,
    "WTTIE": 32,
    "FTWBN": 33,
    "BKWBN": 34,
    "BKPKB": 35,
    "FTPKF": 36,
    "BKFLP": 37,
    "HDHOD": 38,
    "SFPNL": 39,
    "SBPNL": 40,
    "SHSTR": 41,
    "BNPNL": 42,
    "Background": 0,
}

# label2id = {
#     "Background": 0,
#     "Buttons": 1,
#     "Holes": 2,
# }

# movie piece seg
# label2id = {"Background": 0, "Pieces": 1}

id2label = {v: k for k, v in label2id.items()}
ids = list(id2label.keys())


def get_model():
    pretrained_model_name = "nvidia/mit-b0"
    model = SegformerForSemanticSegmentation.from_pretrained(
        pretrained_model_name, id2label=id2label, label2id=label2id
    )
    return model


def get_trainer(prepared_ds, fp16=True, epochs=5, model_name_or_path="segformer"):
    # create here used twice below
    processor = SegformerImageProcessor()

    # dataset drives everything
    ds = prepared_ds["train"].train_test_split(test_size=0.2)
    train_ds = ds["train"]
    test_ds = ds["test"]

    # set up transforms on the dataset
    train_transforms, val_transforms = get_transforms(processor)
    # Set transforms
    train_ds.set_transform(train_transforms)
    test_ds.set_transform(val_transforms)

    lr = 0.00006
    batch_size = 2

    training_args = TrainingArguments(
        model_name_or_path,
        # output_dir=out_dir,
        learning_rate=lr,
        fp16=fp16,
        num_train_epochs=epochs,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size,
        save_total_limit=3,
        evaluation_strategy="steps",
        save_strategy="steps",
        save_steps=20,
        eval_steps=20,
        logging_steps=1,
        eval_accumulation_steps=5,
        load_best_model_at_end=True,
        push_to_hub=False,
        report_to="tensorboard",
        # hub_model_id=hub_model_id,
        # hub_strategy="end",
    )

    comp_metrics = metric_computer(processor)
    trainer = Trainer(
        model=get_model(),
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=test_ds,
        compute_metrics=comp_metrics,
    )

    return trainer


def metric_computer(processor):
    """
    could pass metrics in here if we move them
    """

    def _compute_metrics(eval_pred):
        metric = evaluate.load("mean_iou")

        with torch.no_grad():
            logits, labels = eval_pred
            logits_tensor = torch.from_numpy(logits)
            # scale the logits to the size of the label
            logits_tensor = nn.functional.interpolate(
                logits_tensor,
                size=labels.shape[-2:],
                mode="bilinear",
                align_corners=False,
            ).argmax(dim=1)

            pred_labels = logits_tensor.detach().cpu().numpy()
            # currently using _compute instead of compute
            # see this issue for more info: https://github.com/huggingface/evaluate/pull/328#issuecomment-1286866576
            metrics = metric._compute(
                predictions=pred_labels,
                references=labels,
                num_labels=len(id2label),
                ignore_index=0,
                reduce_labels=processor.do_reduce_labels,
            )

            # add per category metrics as individual key-value pairs
            per_category_accuracy = metrics.pop("per_category_accuracy").tolist()
            per_category_iou = metrics.pop("per_category_iou").tolist()

            print("<<<<<<<<<<<<>>>>>>>>>>>")
            print(per_category_accuracy)

            metrics.update(
                {
                    f"accuracy_{id2label[ids[i]]}": v
                    for i, v in enumerate(per_category_accuracy)
                }
            )
            metrics.update(
                {f"iou_{id2label[ids[i]]}": v for i, v in enumerate(per_category_iou)}
            )

            return metrics

    return _compute_metrics
