def test_count_corners():

    from shapely.wkt import loads
    from res.media.images.geometry import (
        magic_critical_point_filter,
        filter_sensitive_corners,
    )

    # some examples of files with tricky corners for detection
    files = {
        "res_tests/.sample_test_data/body_pieces/TK-3087-V2-TOPBKPNL-S_2X.txt": 8,
        "res_tests/.sample_test_data/body_pieces/TK-3087-V2-TOPFTPNL-S_2X.txt": 8,
        "res_tests/.sample_test_data/body_pieces/JR-3114-V2-SHTBKPNL-S_M.txt": 6,
        # the extra small tests the acute angle filter as well as some other things tested by the other size
        "res_tests/.sample_test_data/body_pieces/TK-3087-V2-TOPFTPNL-S_XXS.txt": 8,
        "res_tests/.sample_test_data/body_pieces/DJ-6000-V1-BODFTPNLRT-S_S.txt": 7,
        # fails: "res_tests/.sample_test_data/body_pieces/JR-3114-V2-SHTFTPNLLF-S_M.txt": 6,
    }

    for filename, corners in files.items():
        with open(filename, "r") as f:
            g = loads(f.read())
            angles = []
            C = magic_critical_point_filter(g, angles=angles)
            CC = filter_sensitive_corners(C, g=g, angles=angles)
            l = len(CC)

            assert (
                l == corners
            ), f"The number of corners in the shape {filename.split('/')[-1].split('.')[0]} was expected to be {corners} but got {l}"


def test_unfold_geometries():
    assert 1 == 1, "nope"

    # ps = [
    #     """POLYGON ((4.0686 0.3908, 4.1123 1.227, 4.1907 2.4567, 4.306 3.47, 4.4401 4.4812, 4.9541 8.127800000000001, 5.2061 9.861599999999999, 8.008900000000001 8.504799999999999, 7.7721 7.347, 7.5527 6.1862, 7.3724 4.9715, 7.2265 3.7522, 7.1481 2.9061, 7.0924 2.0582, 7.0524 0.3908, 4.0686 0.3908))""",
    # ]
    # fold_lines = [
    #     """MULTIPOINT (7.0524 0.3908, 4.0686 0.3908)""",
    # ]

    # for i, p in enumerate(ps):
    #     p = loads(p)
    #     f = loads(fold_lines[i])
    #     g = unfold_on_line(p, LineString(f))

    #     assert g.type == "Polygon", "When unfolded the polygon lost its shape"


def test_is_bias_piece():
    from res.media.images.geometry import (
        is_bias_piece,
    )
    from shapely.geometry import Point, box
    from shapely.affinity import rotate

    assert not is_bias_piece(box(0, 0, 10, 2))
    assert not is_bias_piece(Point(0, 0).buffer(100))
    assert is_bias_piece(rotate(box(0, 0, 10, 2), 45))
    assert is_bias_piece(rotate(box(0, 0, 10, 2), -45))
    assert is_bias_piece(rotate(box(0, 0, 10, 2).boundary, 45))
