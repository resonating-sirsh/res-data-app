import res
import os
import numpy as np
import cv2


def remove_transparency(image):
    background = np.ones_like(image) * 255.0
    alpha = np.stack([image[:, :, 3] / 255.0 for _ in range(3)], axis=2)
    image = np.multiply(image[:, :, 0:3], alpha) + np.multiply(
        background[:, :, 0:3], 1 - alpha
    )
    return image.astype(np.ubyte)


def scale_x_res(image, required_x_res):
    scale = required_x_res / image.shape[1]
    return cv2.resize(
        image,
        (0, 0),
        fx=scale,
        fy=scale,
        interpolation=cv2.INTER_AREA,
    )


def get_image_section(image, y0, y1, x0, x1):
    return image[
        int(y0 * image.shape[0]) : int(y1 * image.shape[0]),
        int(x0 * image.shape[1]) : int(x1 * image.shape[1]),
        :,
    ]
