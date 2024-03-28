# some heuristics for getting at defects
import cv2
import numpy as np


def white_dots(img, k=21, z_thres=4.5):
    f = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    # kind of pseudo z-test the brightness at each pixel
    dt = f - cv2.blur(f, (k, k)).astype("float")
    vs = cv2.blur(dt**2, (k, k))
    z = dt / np.sqrt(vs + 25)
    e = (255 * (z > z_thres) * (f > 120)).astype("uint8")
    # remove shit near edges.
    d = cv2.Canny(cv2.blur(f, (11, 11)), 70, 135)
    d = cv2.dilate(d, np.ones((45, 45), np.uint8))
    e = cv2.bitwise_and(e, cv2.bitwise_not(d))
    # remove shit near the edge of the image.
    e[0:50, :] = 0
    e[-50:, :] = 0
    e[:, 0:50] = 0
    e[:, -50:] = 0
    # draw a lil thing around it.
    e = cv2.dilate(e, np.ones((5, 5), np.uint8))
    cont, hier = cv2.findContours(e, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    return cont


def thread_marks(img):
    # find white spots
    g = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    f = cv2.GaussianBlur(g, (5, 5), 0)
    t = cv2.adaptiveThreshold(
        f, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 4
    )
    t = cv2.dilate(t, np.ones((9, 9)))
    e = cv2.erode(t, np.ones((11, 11))) * (f > 150)
    # remove shit near edges.
    d = cv2.Canny(cv2.GaussianBlur(g, (15, 15), 0), 50, 135)
    d = cv2.dilate(d, np.ones((45, 45), np.uint8))
    e = cv2.bitwise_and(e, cv2.bitwise_not(d))
    # remove shit near the edge of the image.
    e[0:50, :] = 0
    e[-50:, :] = 0
    e[:, 0:50] = 0
    e[:, -50:] = 0
    # draw a lil thing around it.
    e = cv2.dilate(e, np.ones((5, 5), np.uint8))
    cont, hier = cv2.findContours(e, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    return cont
    img = img.copy()
    cv2.drawContours(img, cont, -1, (0, 255, 0), 2)
    return img, len(cont) > 0


def annotate(img):
    defect_colors = {
        thread_marks: (255, 0, 0),
        white_dots: (0, 0, 255),
    }
    defects = []
    for fn, col in defect_colors.items():
        cont = fn(img)
        if len(cont) > 0:
            img = img.copy()
            cv2.drawContours(img, cont, -1, col, 2)
            defects.append(fn.__name__)
    return img, defects
