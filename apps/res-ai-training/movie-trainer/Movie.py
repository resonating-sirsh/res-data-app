import cv2
from PIL import Image
import re
import os
import numpy as np
from shapely.geometry import Polygon, LinearRing, LineString
from shapely.ops import unary_union


def buffer_mask(mask, buffer_size=10):
    """
    we dilate the binary mask as a small int and then return the binary mask
    """
    kernel = np.ones((buffer_size, buffer_size), np.uint8)
    mask = cv2.dilate(mask.astype(np.uint8), kernel, iterations=1)
    return mask > 0


def show_overlay_mask(im: Image, mask: np.ndarray, opacity=200, buffer_size=20):
    """
    given an image an a mask of one or more classes - in this case its assumed that each class is a segmented connected region

         show_overlay_mask(m[9000], seg==3)

    """
    if buffer_size:
        mask = buffer_mask(mask, buffer_size)

    canvas = np.zeros(mask.shape + (4,), dtype=np.uint8)
    canvas[mask] = [250, 10, 10, opacity]
    mask = Image.fromarray(canvas)
    mask = mask.split()[3]
    print(mask.size)
    I = im.copy()
    I.paste(mask, (0, 0), mask)
    return I


def slice_piece_with_mask(
    im: Image,
    mask: np.ndarray,
    full_image_region: bool = False,
    plot=False,
    padding=1,
    buffer_size=20,
):
    """
    splice out the piece, optional shown in its original position
    """
    if buffer_size:
        mask = buffer_mask(mask, buffer_size)
    mask = mask.astype(np.uint8)
    im = im.convert("RGBA")
    im = np.asarray(im)

    masked_region = cv2.bitwise_and(im, im, mask=mask)
    if full_image_region:
        if plot:
            from matplotlib import pyplot as plt

            plt.imshow(masked_region)
        return masked_region
    contours, _ = cv2.findContours(
        mask.astype(np.uint8), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    polygons = [np.squeeze(contour, axis=1).tolist() for contour in contours]
    bounds = np.array(LinearRing(polygons[0]).bounds).astype(int)
    roi = masked_region[
        bounds[1] - padding : bounds[-1] + padding,
        bounds[0] - padding : bounds[-2] + padding,
    ]
    if plot:
        plot.imshow(roi)
    return roi


class Movie:
    def __init__(self, uri):
        self.uri = uri
        self.data = cv2.VideoCapture(uri)
        self._num_frames = int(self.data.get(cv2.CAP_PROP_FRAME_COUNT))

    def __getitem__(self, index):
        self.data.set(cv2.CAP_PROP_POS_FRAMES, index)

        return self.frame

    def __repr__(self):
        return f"{self.uri=}, {self._num_frames}"

    @property
    def frame(self):
        ret, frame = self.data.read()
        return Image.fromarray(frame)

    @staticmethod
    def get_polygons(gray_image, return_image=False, p1=7, p2=10, min_area=2000):
        # Apply adaptive thresholding
        thresh = cv2.adaptiveThreshold(
            gray_image, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY_INV, p1, p2
        )

        contours, _ = cv2.findContours(
            thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
        )
        segmented_image = np.zeros_like(gray_image)
        polygons = []
        for i, contour in enumerate(contours):
            c = np.squeeze(
                contour, axis=1
            )  # Remove the single-dimensional innermost entries
            try:
                polygons.append(Polygon(c))
            except:
                pass
            cv2.drawContours(segmented_image, contours, i, (255, 255, 255), -1)

        polygons = [p.exterior for p in polygons if p.area > min_area]

        if return_image:
            return Image.fromarray(segmented_image)

        return unary_union(polygons)

    def get_gray_frame(self, index, border_size=None):
        self.data.set(cv2.CAP_PROP_POS_FRAMES, index)
        ret, frame = self.data.read()
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        if border_size:
            frame = cv2.copyMakeBorder(
                frame,
                border_size,
                border_size,
                border_size,
                border_size,
                cv2.BORDER_CONSTANT,
                value=(0, 0, 0),
            )
        return Image.fromarray(frame)

    def get_frame_polygons(self, index, p1=7, p2=10, return_image=True):
        return Movie.get_polygons(
            np.asarray(self.get_gray_frame(index)),
            p1=p1,
            p2=p2,
            return_image=return_image,
        )

    def get_segmented_frame(self, index, return_image=False):
        from skimage.segmentation import felzenszwalb

        im = self.get_frame_polygons(index)
        im = np.asarray(im).astype(np.uint8)

        kernel = np.ones((15, 15), np.uint8)
        im = cv2.erode(im, kernel, iterations=1)

        segments = felzenszwalb(im, scale=100, sigma=0.01, min_size=3000).astype(
            np.uint8
        )
        # here we get rid of grid lines and other artifacts
        return segments if not return_image else Image.fromarray(segments * 100)

    def iterate_segmented(self):
        for i in range(self._num_frames):
            s = self.get_segmented_frame(i, return_image=False)
            yield s

    def iterate_frame_polygons(self):
        for i in range(self._num_frames):
            s = self.get_segmented_frame(i, return_image=False)
            yield s
