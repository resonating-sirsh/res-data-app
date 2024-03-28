import res
import numpy as np
import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_material_properties(material_codes):
    """
    query the graph given some codes - get some material props
    scale some things
    """

    GET_MATERIAL_QUERY = """
                query gm(  $where: MaterialsWhere!  ) {
                    materials(first: 100,   where: $where ) {
                        materials {
                            code
                            cuttableWidth
                            offsetSizeInches
                            fabricType
                            printFileCompensationLength
                            printFileCompensationWidth
                            paperMarkerCompensationWidth
                            paperMarkerCompensationLength
                        }
                    }
                }
                """

    data = pd.DataFrame(
        res.connectors.load("graphql").query_with_kwargs(
            GET_MATERIAL_QUERY, where={"code": {"isAnyOf": material_codes}}
        )["data"]["materials"]["materials"]
    ).rename(
        columns={
            "code": "key",
            "cuttableWidth": "cuttable_width",
            "offsetSizeInches": "offset_size",
            "fabricType": "fabric_type",
            "paperMarkerCompensationWidth": "paper_marker_compensation_width",
            "paperMarkerCompensationLength": "paper_marker_compensation_length",
            "printFileCompensationLength": "compensation_length",
            "printFileCompensationWidth": "compensation_width",
        }
    )

    # scale to the convention we used from the fields in the graph we chose
    for c in [
        "paper_marker_compensation_width",
        "paper_marker_compensation_length",
        "compensation_length",
        "compensation_width",
    ]:
        data[c] /= 100.0

    data = data.replace(np.nan, None)
    data = data.where(pd.notnull(data), None)

    return {record["key"]: record for record in data.to_dict("records")}


def get_material_description_label(code, size=(2500, 1200)):
    """
    this descriptive label is usually printed on material walls
    """

    from PIL import Image, ImageFont, ImageDraw
    from res.utils.env import FONT_LOCATION

    GET_MATERIAL_QUERY = """
                query gm(  $where: MaterialsWhere!  ) {
                    materials(first: 1,   where: $where ) {
                        materials {
                            name
                            code
                            fabricType
                            weight
                            content
                            materialTaxonomyCode
                        }
                    }
                }
                """

    data = res.connectors.load("graphql").query_with_kwargs(
        GET_MATERIAL_QUERY, where={"code": {"isAnyOf": [code]}}
    )["data"]["materials"]["materials"][0]

    txt = Image.new("RGBA", size, (255, 255, 255, 255))
    # get a font
    title = ImageFont.truetype(f"{FONT_LOCATION}/Noli-SemiBold.ttf", 150)
    para = ImageFont.truetype(f"{FONT_LOCATION}/Noli-Book 6.otf", 100)

    d = ImageDraw.Draw(txt)

    # draw text, half opacity
    d.text((10, 10), data["name"], font=title, fill="black")
    d.text((10, 250), data["content"], font=para, fill="black")
    d.text((10, 375), f"({data['code']})", font=para, fill="black")

    d.text((10, 600), data["materialTaxonomyCode"], font=para, fill="black")
    d.text((10, 725), f"{data['weight']} GSM", font=para, fill="black")

    return txt
