import res

try:
    from .BodyPiecesModel import BodyPiecesModel
except:
    # we dont always load all the stuff on docker images
    print("Failing to load all models - check deps")


def load(model_key, eval_model=True):
    """
    s3.upload('piece_model.pth', "s3://res-data-platform/trained-models/piece-classification/piece-model-10class-01.pth")

    this is a dummy method for now
    in reality the models would be loaded from somewhere else for construction and then the weights would be loaded from s3
    """
    import torch
    from torch import nn
    import io
    from res.media.images import make_square_thumbnail

    def transform_image(image_or_bytes):
        from PIL import Image
        import torchvision.transforms as transforms
        from PIL import ImageOps

        """
        supply a color image of any size and it will be transformed
        """
        if not isinstance(image_or_bytes, Image.Image):
            image_or_bytes = Image.open(io.BytesIO(image_or_bytes))

        image_or_bytes = make_square_thumbnail(image_or_bytes)

        # just do the tensor thing and trust our other image ops
        my_transforms = transforms.Compose(
            [
                transforms.ToTensor(),
            ]
        )

        # this image model is very sensitive to being exactly as we read it including the inversion
        # may be interesting to try is with different types of inversions for robustness
        image = ImageOps.invert(image_or_bytes.convert("L"))
        # unsqueeze important for shape 1 * 128 * 128 because the model takes a batch of images in general
        return my_transforms(image).unsqueeze(0)

    class NeuralNetwork(nn.Module):
        def __init__(self):
            super().__init__()
            self.flatten = nn.Flatten()
            self.linear_relu_stack = nn.Sequential(
                nn.Linear(128 * 128, 512),
                nn.ReLU(),
                nn.Linear(512, 512),
                nn.ReLU(),
                # 10 is the number of classes we trained here
                nn.Linear(512, 10),
            )

        def forward(self, x):
            x = self.flatten(x)
            logits = self.linear_relu_stack(x)
            return logits

    class Bundle:
        def __init__(self, model, classes, image_transformer):
            self.model = model
            self.classes = dict(zip(range(classes)), classes)
            self.image_transformer = image_transformer

    s3 = res.connectors.load("s3")
    with s3.file_object(
        f"s3://res-data-platform/trained-models/{model_key}.pth", "rb"
    ) as f:
        d = torch.load(f)
        model = NeuralNetwork()
        model.load_state_dict(d)
        model.eval()
        classes = s3.read(
            f"s3://res-data-platform/trained-models/{model_key}.class_names.json"
        )
        return Bundle(model, classes, transform_image)
