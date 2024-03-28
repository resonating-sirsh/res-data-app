import cv2 as cv
from PIL import Image


class CropLayer(object):
    def __init__(self, params, blobs):
        self.xstart = 0
        self.xend = 0
        self.ystart = 0
        self.yend = 0

    # Our layer receives two inputs. We need to crop the first input blob
    # to match a shape of the second one (keeping batch size and number of channels)
    def getMemoryShapes(self, inputs):
        inputShape, targetShape = inputs[0], inputs[1]
        batchSize, numChannels = inputShape[0], inputShape[1]
        height, width = targetShape[2], targetShape[3]

        self.ystart = (inputShape[2] - targetShape[2]) // 2
        self.xstart = (inputShape[3] - targetShape[3]) // 2
        self.yend = self.ystart + height
        self.xend = self.xstart + width

        return [[batchSize, numChannels, height, width]]

    def forward(self, inputs):
        return [inputs[0][:, :, self.ystart : self.yend, self.xstart : self.xend]]


class Sketchify:
    def __init__(self):

        self.net = cv.dnn.readNetFromCaffe(
            "/Users/sirsh/Downloads/deploy.prototxt",  # https://github.com/s9xie/hed/blob/master/examples/hed/deploy.prototxt
            # https://github.com/s9xie/hed?tab=readme-ov-file
            "/Users/sirsh/Downloads/hed_pretrained_bsds.caffemodel",
        )
        cv.dnn_registerLayer("Crop", CropLayer)

    def __call__(self, path_or_image, **kwargs):
        return self._sketchify(path_or_image, **kwargs)

    def _sketchify(
        self,
        path_or_image,
        invert=True,
        mean=(104.00698793, 186.66876762, 122.67891434),
    ):

        img = (
            cv.imread(path_or_image)
            if isinstance(path_or_image, str)
            else path_or_image
        )

        (H, W) = img.shape[:2]

        blob = cv.dnn.blobFromImage(
            img, scalefactor=1.0, size=(W, H), mean=mean, swapRB=False, crop=False
        )
        self.net.setInput(blob)
        hed = self.net.forward()
        hed = cv.resize(hed[0, 0], (W, H))
        hed = (255 * hed).astype("uint8")
        if invert:
            hed = 255 - hed
        hed = Image.fromarray(hed)

        if isinstance(path_or_image, str):
            hed.save(path_or_image.replace("/images/", "/hed/"))
        return hed
