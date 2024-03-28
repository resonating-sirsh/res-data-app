from transformers import SegformerImageProcessor, SegformerForSemanticSegmentation
import numpy as np

classes = {
    "BKPNL": 1,
    "SLPNL": 2,
    "NKBND": 3,
    "FTPNL": 4,
    "NKCLR": 5,
    "SLCUF": 6,
    "BKPKT": 7,
    "BKYKE": 8,
    "FSPNL": 9,
    "FTPKB": 10,
    "WTWBN": 11,
    "FTFLE": 12,
    "SLPLK": 13,
    "NKCLS": 14,
    "FTPKT": 15,
    "FTPLK": 16,
    "STPNL": 17,
    "FTFLP": 18,
    "BSPNL": 19,
    "SUPNL": 20,
    "FTFAC": 21,
    "BNPNL": 22,
    "BNFAC": 23,
    "SDPKB": 24,
    "WTTIE": 25,
    "BKFLP": 26,
    "SDGSS": 27,
    "BKLOP": 28,
    "BKPKB": 29,
    "SDPKT": 30,
    "SDFLP": 31,
    "FTWBN": 32,
    "BKWBN": 33,
    "FTYKE": 34,
    "WTTAB": 35,
    "SDPNL": 36,
    "FTPKF": 37,
    "BYPNL": 38,
    "SHSTR": 39,
    "Background": 0,
}


class SegFormer:
    def __init__(self, check_point="/Users/sirsh/models/sketch_seg/checkpoint-7580"):
        self._path = check_point
        # processor = SegformerImageProcessor.from_pretrained("nvidia/segformer-b0-finetuned-ade-512-512")
        self.processor = SegformerImageProcessor.from_pretrained(
            f"{check_point}/config.json"
        )
        self.model = SegformerForSemanticSegmentation.from_pretrained(f"{check_point}")
        self.classes_to_ids = classes
        self.ids_to_classes = {v: k for k, v in classes.items()}

    def predict_image(self, example, as_pil=False):
        from torch import nn

        inputs = self.processor(images=example, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits  # shape (batch_size, num_labels, height/4, width/4)

        # First, rescale logits to original image size
        upsampled_logits = nn.functional.interpolate(
            logits,
            size=example.size[::-1],  # (height, width)
            mode="bilinear",
            align_corners=False,
        )

        # Second, apply argmax on the class dimension
        pred_seg = upsampled_logits.argmax(dim=1)[0]
        pred_seg = pred_seg.numpy().astype("uint8")

        ids = np.unique(pred_seg)
        res.utils.logger.info([self.ids_to_classes.get(x) for x in ids])

        if as_pil:
            return Image.fromarray(pred_seg * 100)
        return pred_seg

    def overlay_class_masks(iself, m, mask_im, filters=[]):
        """
        overlay labels on an image
        """
        F = im.copy().convert("RGBA")
        g = np.asarray(mask_im)
        print(np.unique(g))
        for i, k in enumerate(np.unique(g)):
            if k == 0:
                continue
            if filters and k not in filters:
                continue
            label = self.ids_to_classes[k]
            canvas = np.zeros(g.shape + (4,), dtype=np.uint8)
            # color todo
            canvas[g == k] = [k, 125, 255, 25]
            print(label, k)

            mask = Image.fromarray(canvas * 50 * (1 + i))
            mask = mask.split()[3]
            F.paste(mask, (0, 0), mask)
        return F
