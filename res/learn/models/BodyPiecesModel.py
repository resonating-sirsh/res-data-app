from res.learn.models.trainers.VitTrainer import *
import json
import torchvision.transforms as T
from PIL import Image
import pandas as pd
import numpy as np
from res.media.images.outlines import get_padded_image_mask
from res.media.images.geometry import rotate, shift_geometry_to_origin
import res


# 11400 best so far
class BodyPiecesModel:
    def __init__(self, path="/Users/sirsh/models/bp1/checkpoint-11400", clu=None):
        self._path = path
        self._feature_extractor = ViTFeatureExtractor.from_pretrained(path)

        # the component name lookup
        self._clu = clu

        # load a model check point - this is the check point data
        with open(f"{path}/config.json") as f:
            self._cnf = json.load(f)
            self._id2label = self._cnf["id2label"]
            self._label2id = self._cnf["label2id"]

        self._model = ViTForImageClassification.from_pretrained(
            path,
            num_labels=len(self._id2label),
            id2label=self._id2label,
            label2id=self._label2id,
        )

        # gotta be a little careful with labels - the integer sorted and then retrieve the keys which are strings
        # logits will be returned on that integer index so we want to sort this - same as if we did
        #   prepared_ds['validation'].features["labels"].names

        self.labels = [self._id2label[str(i)] for i in range(len(self._id2label))]

    def confidence(self, output_distribution):
        """
        simple inbuilt confidence to compare two models e.g. pieces rotated or not
        """
        return -np.sum(output_distribution * np.log2(output_distribution))

    def predict_shape(self, g, plot=False):
        """
        A wrapper to predict a shape in shapely format (for now)
        converts to an image and uses the predict image method
        """
        im = get_padded_image_mask(g, make_square=True)  # .thumbnail((256, 256))
        return self.predict_image(im, plot=plot)

    # def is_perfect_rectangle(rectangle):
    #     if len(rectangle) != 4:
    #         return False

    #     x_coords, y_coords = zip(*rectangle)
    #     min_x, min_y = min(x_coords), min(y_coords)
    #     max_x, max_y = max(x_coords), max(y_coords)

    #     expected_area = (max_x - min_x) * (max_y - min_y)
    #     actual_area = abs((rectangle[1][0] - rectangle[0][0]) * (rectangle[2][1] - rectangle[0][1]))

    #     return expected_area == actual_area

    def predict_image(self, im: Image, plot=False):
        inputs = self._feature_extractor(images=im, return_tensors="pt")[
            "pixel_values"
        ][0]
        # get the model result
        y = self._model(inputs.unsqueeze(0))
        d = list(zip(self.labels, list(y.logits.detach().numpy()[0])))
        # plot it
        if plot:
            pd.DataFrame(d, columns=["label", "probability"]).set_index(
                "label"
            ).sort_values("probability", ascending=False).plot(
                kind="bar", figsize=(20, 5)
            )

        return dict(d)

    def __call__(self, data):
        return self._model(data)

    def resolve_name(self, code):
        part = code[:2]
        comp = code[2:]
        clu = self._clu

        def cl(c):
            return "" if not c else c.lstrip().rstrip()

        return f"{cl(clu.get(part)) } {cl(clu.get(comp) )}"

    def format_result(self, pred_matrix):
        """
        some arbitrary formatting
        """

        def row_as_probs(row):
            stuff = [(k, v) for k, v in row.items()]
            stuff = sorted(stuff, key=lambda x: x[-1])
            stuff = pd.DataFrame(
                [
                    {
                        "piece_name": self.resolve_name(t[0]),
                        "piece_code": t[0],
                        "weight": t[1],
                    }
                    for t in stuff[-5:][::-1]
                ]
            )
            stuff["probability"] = stuff["weight"] / (stuff["weight"].sum())
            return stuff.to_dict("records")

        def _confidence(distro):
            vals = [d["probability"] for d in distro]
            return self.confidence(vals)

        piece_probs = []
        for idx, row in enumerate(pred_matrix.to_dict("records")):
            p = {"piece_index": idx, "distribution": row_as_probs(row)}
            # confidence is an entropy measure over scores so we can try multiple models
            p["confidence_score"] = _confidence(p["distribution"])
            piece_probs.append(p)

        return piece_probs

    def classify_piece_geometries(
        self, df: pd.DataFrame, piece_rotation: int = 0
    ) -> pd.DataFrame:
        """
        given a dataframe with geometries we will classify them by first masking
        returns a piece to component prediction matrix in json records

        We apply some heuristics
        - generally for perfectly rectangular pieces we cannot say much out of context. some very long pieces may be bindings for center front plackets
          these usually "pair" with something
        - for very small pieces its worth adding perspective by putting the piece in a square tile or else they look like big panels
          (we could build this into a model but would require more training time)
        - Some pieces may be "confused" in a first pass and its worth checking the 90 degree rotation based on DXF conventions for e.g. sleeve area
        """

        res.utils.logger.info(f"building thumbs")
        records = df.to_dict("records")
        """
        We are apply the heuristic which is not solid: if the piece is small then pad into a square otherwise leave as is
        """
        images = [
            get_padded_image_mask(
                rotate(row["geometry"], piece_rotation),
                make_square=row["piece_area_inches"] < 100,
            )
            for row in records
        ]
        for image in images:
            image.thumbnail((256, 256))

        inputs = self._feature_extractor(images=images, return_tensors="pt")[
            "pixel_values"
        ]

        res.utils.logger.info(f"Predicting batch")
        y = self._model(inputs)

        # something human readable
        labels = (
            [f"{self.resolve_name(c)} ({c})" for c in self.labels]
            if self._clu
            else self.labels
        )
        pred_matrix = pd.DataFrame(y.logits.detach().numpy(), columns=labels)
        pred_matrix_formatted = self.format_result(pred_matrix)
        return pred_matrix_formatted

    def classify_piece_geometries_iterative(
        self, df: pd.DataFrame, plot: bool = False, small_piece_area_inches: int = 100
    ):
        """
        this is like the batch but easier to test for situations where we wanna mess with the shape
        proper testing to should lead to a better overall model that needs no messaging
        then doing batch inference that we can trust makes more sense. we will get to that.

        """
        solution = []
        from tqdm import tqdm

        for i, record in tqdm(enumerate(df.to_dict("records"))):
            small_piece = record["piece_area_inches"] < small_piece_area_inches
            # not not
            g = shift_geometry_to_origin(rotate(record["geometry"], 0))
            im = get_padded_image_mask(g, make_square=small_piece)
            im.thumbnail((256, 256))
            r = self.predict_image(im, plot=plot)
            r = self.format_result(pd.DataFrame([r]))[0]
            # im.save(f"/Users/sirsh/Downloads//k/{i}.png")
            ##
            g = shift_geometry_to_origin(rotate(record["geometry"], 90))
            im = get_padded_image_mask(g, make_square=small_piece)
            im.thumbnail((256, 256))
            r2 = self.predict_image(im, plot=plot)
            r2 = self.format_result(pd.DataFrame([r2]))[0]

            # im.save(f"/Users/sirsh/Downloads/k/r_{i}.png")
            dist = dict(r["distribution"][0])
            dist["uncertainty_score"] = r["confidence_score"]
            res.utils.logger.debug(f"piece {i} is first model = {dist['piece_code']}")

            if r["confidence_score"] > r2["confidence_score"]:
                dist = dict(r2["distribution"][0])
                dist["uncertainty_score"] = r2["confidence_score"]
                res.utils.logger.debug(
                    f"piece {i} is second model = {dist['piece_code']} with higher confidence"
                )
            dist["piece_index"] = i
            solution.append(dist)

        return pd.DataFrame(solution)
