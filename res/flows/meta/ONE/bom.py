import pandas as pd
import res
from tenacity import retry, stop_after_attempt, wait_fixed


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_thread_colors(style_code):
    # we have to go back to the style because the stylecode listted in the BOM is a lie
    gr = res.connectors.load("graphql").query_with_kwargs(
        """
        query ($styleCode: WhereValue!) {
            styles(first:10, where: {code: {is: $styleCode}}) {
                styles {
                    styleBillOfMaterials {
                        trimItem {
                            longName
                        }
                    }
                }
            }
        }
        """,
        styleCode=style_code,
    )
    return [
        r["trimItem"]["longName"]
        for s in gr["data"]["styles"]["styles"]
        for r in s["styleBillOfMaterials"]
        if r["trimItem"]
        and "longName" in r["trimItem"]
        and "HILO" in r["trimItem"]["longName"]
        and "ELASTIC" not in r["trimItem"]["longName"]
    ]


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_body_bom(body_code):
    """
    rough work: get and validate body trims in the body node - store them on the body

    """
    Q = f"""
    query getBoM  {{
            billOfMaterials(first:100,where:{{body: {{is: "{body_code}"}}}} ){{
                billOfMaterials{{        
                    code
                    name 
                    sizeScale 
                    type 
                    trimType 
                    size 
                    unit 
                    createdAt 
                    updatedAt 
                    trim {{
                            name
                        }}
                        
                    trimCategory 
                    trimTaxonomyId 
                    quantity 
                    trimTaxonomy {{
                            name
                        }}
                        
                    flagForReview 
                    flagForReviewReason 
                    styleBomFriendlyName 
                    trimLength 
                    cordDiameter 
                    trimQuantity 
                    trimLengthUnitOfMeasurement 
                    usage 
                    body 
                    
                    fusingId 
                    fusing {{
                            name
                            code
                        }}
                        
                    pieceType 
                    fusingUsage 
                    pieceNameCode 
                    isFusingUpdated 
                }}
            }}
        }}
    """

    gapi = res.connectors.load("graphql")
    data = gapi.query_with_kwargs(Q)

    if data is None or "data" not in data:
        # something is broke
        return None

    data = data["data"]["billOfMaterials"]["billOfMaterials"]

    if len(data) == 0:
        return None

    data = pd.DataFrame(data)
    # sizeSpecificBoms
    d = data[
        [
            "code",
            "name",
            "fusing",
            "trimType",
            "type",
            "pieceNameCode",
            "trimLength",
            "quantity",
            "unit",
            "sizeScale",
        ]
    ]
    trims = d.to_dict("records")
    d["pieces"] = d["pieceNameCode"].map(
        lambda x: [r.strip() for r in x.split(",")] if isinstance(x, str) else []
    )
    ptrims = dict(d[["pieces", "fusing"]].explode("pieces").dropna().values)
    return {"data": trims, "piece_fusing": ptrims}
