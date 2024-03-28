"""
Keypoints generated are normally of type map collections as they are metadata not shape collections per se
"""

from skimage.feature import corner_harris, corner_subpix, corner_peaks


def _harris_corners(image, **kwargs):
    """
    harris corners:
    https://scikit-image.org/docs/dev/auto_examples/features_detection/plot_corner.html

    """
    # override from kwargs
    min_distance = 5
    threshold_rel = 0.02  # was 0.02 - make bigger to be more accepting
    window_size = 15
    harris_sensitivity = 0.05  # 365

    coords = corner_peaks(
        corner_harris(image, k=harris_sensitivity),
        min_distance=min_distance,
        threshold_rel=threshold_rel,
    )
    coords_subpix = corner_subpix(image, coords, window_size=window_size)

    return {"peaks": coords, "corners": coords_subpix}


def corners(image_collection, **kwargs):
    """
    Using harris corners (by default for now)
    """
    corner_func = _harris_corners

    for image in image_collection:
        yield corner_func(image)


def combined_keypoints(image_collection, **kwargs):
    all_corners = list(corners(image_collection))
    # other things
    # combine results
    results = all_corners

    for result in results:
        yield result


def label_factory(image, text, transform):
    # temp
    pass
    # img = cv2.imread(input_file_path, cv2.IMREAD_UNCHANGED)
    # one_code_img = cv2.imread(one_code_input_file_path, cv2.IMREAD_UNCHANGED)
    # one_code_img = cv2.resize(one_code_img, (LABEL_MAX_HEIGHT, LABEL_MAX_HEIGHT), interpolation = cv2.INTER_AREA)
    # one_number_img = generate_text_label(one_number, (255,255,255), (0,0,0,0), 1, 3, (0,0,0), LABEL_MAX_HEIGHT)
    # label = np.concatenate((one_number_img, one_code_img), axis=1)
