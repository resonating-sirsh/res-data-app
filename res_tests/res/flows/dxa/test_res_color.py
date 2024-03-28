from res.flows.dxa.res_color import (
    add_overlay_functions_to_data,
    RES_COLOR_FUNCTION_FIELD,
)
import res
import pytest
import pandas as pd


@pytest.mark.data
def test_add_overlay_functions_to_data():
    from res.flows.dxa import res_color

    return

    im = "s3://meta-one-assets-prod/color_on_shape/jr-3108_v3_pinuoh/1zzxs/pieces/00997fe2-47af-4e0f-a251-13dc04b14efd.png"
    # todo load a really small outline mask image from somewhere

    data = [
        {
            "key": "1000123",
            "piece_key": "CC-4047-V2-FT_SM",
            "body_version": "v2",
            "body_code": "cc_4047",
            "dxf_size": "SM",
            "color": "pertif",
            "size": "zzz04",
            # "material": ""
        }
    ]

    data = pd.DataFrame(data)

    data = add_overlay_functions_to_data(data)

    f = data.iloc[0][RES_COLOR_FUNCTION_FIELD]

    f(im)
