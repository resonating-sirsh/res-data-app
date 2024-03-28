import pandas as pd
import datetime
import requests
import res
import os
import re
from fontTools.ttLib import TTFont
import re
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils.logging import logger
from res.utils.meta.sizes.sizes import get_sizes_from_aliases
from res.media.images.outlines import analyze_artwork
import res.flows.meta.sync_artworks as sync_artworks
import res.flows.dxa.apply_dynamic_color as apply_dynamic_color
import res.flows.meta.body.graphql_queries as body_graphql_queries
from res.flows.dxa.bertha import upsert_style as c1_upsert_style
from res.flows.meta.style.style import assign_trims_to_first_one_style
from schemas.pydantic.design import (
    Artwork,
    DpiUri,
    ColorBase,
    CreateArtwork,
    CreateArtworkResponse,
    Body3DFiles,
    Color,
    CreateColor,
    UpdateColor,
    DesignData,
    UserDesign,
    GeoJson,
    PieceGeoJsons,
    DesignCatalogItem,
    Placement,
    Pin,
    Font,
    Placeholder,
    # StyleCatalogItem,
)


class DesignException(Exception):
    def __init__(self, error_code: str, message: str, detail: str):
        self.error_code = error_code
        self.message = message
        self.detail = detail

    def __str__(self) -> str:
        return super().__str__() + f" {self.error_code}: {self.message} - {self.detail}"


APPLY_COLOR_REQUEST_NOT_FOUND = "APPLY_COLOR_REQUEST_NOT_FOUND"
BAD_PARAMETERS = "BAD_PARAMETERS"
BODY_NOT_FOUND = "BODY_NOT_FOUND"
DESIGN_NOT_FOUND = "DESIGN_NOT_FOUND"
INVALID_EDITOR_TYPE = "INVALID_EDITOR_TYPE"
MISSING_ARTWORKS = "MISSING_ARTWORKS"
MISSING_FONTS = "MISSING_FONTS"
COLOR_NOT_FOUND = "COLOR_NOT_FOUND"
SKU_NOT_FOUND = "SKU_NOT_FOUND"
UNSUPPORTED_CHARACTERS = "UNSUPPORTED_CHARACTERS"


def _remove_body_and_version_from_piece_name(piece_name):
    parts = piece_name.split("-")
    return piece_name if len(parts) < 3 else "-".join(parts[3:])


def artwork_dpi_query_fragment(sign=False, max_dpi=None):
    signed = "signed_" if sign else ""
    dpi_300 = f"{signed}dpi_300_uri" if max_dpi is None or max_dpi >= 300 else ""
    dpi_72 = f"{signed}dpi_72_uri" if max_dpi is None or max_dpi >= 72 else ""
    dpi_36 = f"{signed}dpi_36_uri" if max_dpi is None or max_dpi >= 36 else ""
    dpis_query = f"""
        {dpi_300}
        {dpi_72}
        {dpi_36}
    """

    return dpis_query


def artwork_to_Artwork(artwork):
    artwork["colors"] = [
        ColorBase(**c["color"]) for c in artwork.get("colors_artworks", [])
    ]
    dpi_uris = []
    for dpi in ["300", "72", "36"]:
        if artwork.get(f"dpi_{dpi}_uri"):
            dpi_uris.append(DpiUri(dpi=dpi, uri=artwork[f"dpi_{dpi}_uri"]))
        if artwork.get(f"signed_dpi_{dpi}_uri"):
            dpi_uris.append(DpiUri(dpi=dpi, uri=artwork[f"signed_dpi_{dpi}_uri"]))
    artwork["uris"] = dpi_uris
    return Artwork(**artwork)


def to_color_artwork(color_id, artwork_id):
    return {
        "id": str(
            res.utils.uuid_str_from_dict(
                {"color_id": str(color_id), "artwork_id": artwork_id}
            )
        ),
        "color_id": str(color_id),
        "artwork_id": artwork_id,
    }


def get_artworks(
    id=None,
    search=None,
    brand=None,
    name=None,
    color_code=None,
    sign_uris=False,
    limit: int = 10,
    offset: int = 0,
    max_dpi: int = None,
    include_deleted: bool = False,
) -> list[Artwork]:
    logger.info(f"get_artworks name={name} brand={brand} id={id}")

    id_query = f'id: {{_eq: "{id}"}}' if id is not None else None

    brand_query = f'brand: {{_ilike: "{brand}"}}' if brand is not None else None
    name_query = f'name: {{_ilike: "%{name}%"}}' if name is not None else None
    color_query = (
        f'colors_artworks: {{color: {{code: {{_ilike: "%{color_code}%"}}}}}}'
        if color_code is not None
        else None
    )
    search_query = None
    if search:
        search_query = f"""
            _or: [
                {{brand: {{_ilike: "%{search}%"}}}},
                {{name: {{_ilike: "%{search}%"}}}},
                {{description: {{_ilike: "%{search}%"}}}},
                {{colors_artworks: {{color: {{code: {{_ilike: "%{search}%"}}}}}}}}
            ]
        """

    other_queries = [
        q for q in [brand_query, name_query, color_query, search_query] if q
    ]

    where = id_query if id_query else ", ".join(other_queries)

    if not include_deleted:
        where = f"deleted_at: {{_is_null: true}}, {where}"

    dpis_fragment = artwork_dpi_query_fragment(sign_uris, max_dpi)

    # logger.info(f"where = {where}")
    GET_ARTWORKS_QUERY = f"""
        query latestArtwork {{
            meta_artworks(where: {{
                {where}
            }}, 
                order_by: {{created_at: desc}}, 
                limit: {limit},
                offset: {offset}
            ) {{
                id
                name
                brand
                description
                original_uri
                {dpis_fragment}
                metadata
                created_at
                updated_at
                deleted_at
                colors_artworks {{
                    color {{
                        id
                        code
                        name
                        brand
                    }}
                }}
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute(GET_ARTWORKS_QUERY)
    artworks: list[Artwork] = [artwork_to_Artwork(a) for a in result["meta_artworks"]]

    return artworks


def add_artwork(artwork: CreateArtwork):
    if not artwork.s3_uri and not artwork.external_uri:
        raise DesignException(
            BAD_PARAMETERS,
            "Invalid image uri",
            "You must provide either an s3 uri or an external uri",
        )

    s3_uri = artwork.s3_uri
    # if it's external, upload to s3 first
    if artwork.external_uri:
        s3_uri = sync_artworks.upload_external_uri_to_s3(
            artwork.external_uri, brand=artwork.brand
        )
        if s3_uri is None:
            raise DesignException(
                BAD_PARAMETERS,
                "Invalid image uri",
                f"Could not download image from {artwork.external_uri}",
            )

    analysis = analyze_artwork(s3_uri, include_tile_preview=artwork.tile_preview)

    id = None
    if "errors" not in analysis:
        id = sync_artworks.submit_convert_and_save(
            artwork.name,
            source_dpi=300,
            source=s3_uri,
            brand=artwork.brand,
            source_id="external" if artwork.external_uri else "api",
            redo_if_exists=True,
            metadata=(
                {"source_uri": artwork.external_uri} if artwork.external_uri else {}
            ),
        )

    return CreateArtworkResponse(id=id, **analysis)


def delete_artwork(id: str):
    # get the current time
    now = datetime.datetime.now().isoformat()

    DELETE_ARTWORK_MUTATION = """
        mutation delete_artwork($id: uuid!, $deleted_at: timestamptz!) {
            update_meta_artworks_by_pk(pk_columns: {id: $id}, _set: {deleted_at: $deleted_at}) {
                id
                deleted_at
            }
        }
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        DELETE_ARTWORK_MUTATION, id=str(id), deleted_at=now
    )
    return result.get("update_meta_artworks_by_pk") != None


def update_an_artwork(
    id: str,
    deleted: bool = False,
    name: str = None,
    brand: str = None,
    description: str = None,
):
    # get the current time
    now = datetime.datetime.now().isoformat()

    _set = {
        "name": name,
        "brand": brand,
        "description": description,
    }
    _set = {k: v for k, v in _set.items() if v is not None}
    _set["deleted_at"] = now if deleted else None
    _set["updated_at"] = now
    _set = ", ".join(
        [f"""{k}: {'null' if not v else '"'+v+'"'}""" for k, v in _set.items()]
    )

    UPDATE_ARTWORK_MUTATION = f"""
        mutation update_artwork($id: uuid!) {{
            update_meta_artworks_by_pk(pk_columns: {{id: $id}}, _set: {{{_set}}}) {{
                id
                deleted_at
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(UPDATE_ARTWORK_MUTATION, id=str(id))

    return result.get("update_meta_artworks_by_pk") != None


def check_for_missing_artworks(artwork_ids: list[str]):
    # make sure all the artworks exist first, get their ids
    EXISTING_ARTWORK_IDS = """
        query artworkCount($ids: [uuid!]!) {
            meta_artworks(where: {id: {_in: $ids}}) {
                id
            }
        }
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute(
        EXISTING_ARTWORK_IDS, {"ids": [str(id) for id in artwork_ids]}
    )
    missing = set(artwork_ids) - set([a["id"] for a in result["meta_artworks"]])
    if missing:
        raise DesignException(
            MISSING_ARTWORKS,
            "Artwork ids provided do not exist",
            f"Artwork ids provided do not exist: {missing}",
        )


def get_fonts_by_ids(font_ids: list[str]):
    GET_FONTS_QUERY = f"""
        query getFonts($ids: [uuid!]!) {{
            meta_fonts(where: {{id: {{_in: $ids}}}}
            ) {{
                id
                name
                brand
                uri: signed_uri
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(GET_FONTS_QUERY, ids=font_ids)

    missing = set(font_ids) - set([a["id"] for a in result["meta_fonts"]])
    if missing:
        raise DesignException(
            MISSING_FONTS,
            "Font ids provided do not exist",
            f"Font ids provided do not exist: {missing}",
        )

    fonts: dict[Font] = {f["id"]: Font(**f) for f in result["meta_fonts"]}

    return fonts


def get_artworks_by_ids(artwork_ids: list[str], max_dpi: int = None):
    GET_ARTWORKS_QUERY = f"""
        query getArtworksList($ids: [uuid!]!) {{
            meta_artworks(where: {{id: {{_in: $ids}}}}
            ) {{
                id
                name
                brand
                description
                {"dpi_300_uri: signed_dpi_300_uri" if max_dpi is None or max_dpi >= 300 else ""}
                {"dpi_72_uri: signed_dpi_72_uri" if max_dpi is None or max_dpi >= 72 else ""}
                {"dpi_36_uri: signed_dpi_36_uri" if max_dpi is None or max_dpi >= 36 else ""}
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(GET_ARTWORKS_QUERY, ids=artwork_ids)

    missing = set(artwork_ids) - set([a["id"] for a in result["meta_artworks"]])
    if missing:
        raise DesignException(
            MISSING_ARTWORKS,
            "Artwork ids provided do not exist",
            f"Artwork ids provided do not exist: {missing}",
        )

    artworks: dict[Artwork] = {
        a["id"]: artwork_to_Artwork(a) for a in result["meta_artworks"]
    }

    return artworks


def get_artworks_source_id_mapping(source_ids: list[str], max_dpi: int = None):
    clause = ", ".join(
        [
            f"""{{metadata: {{ _contains: {{ source_id: "{id}" }}}}}},"""
            for id in source_ids
        ]
    )
    GET_ARTWORKS_QUERY = f"""
        query getArtworksList {{
            meta_artworks(where: {{_or: [{clause}]}}
            ) {{
                id
                name
                brand
                description
                {"dpi_300_uri: signed_dpi_300_uri" if max_dpi is None or max_dpi >= 300 else ""}
                {"dpi_72_uri: signed_dpi_72_uri" if max_dpi is None or max_dpi >= 72 else ""}
                {"dpi_36_uri: signed_dpi_36_uri" if max_dpi is None or max_dpi >= 36 else ""}
                metadata
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(GET_ARTWORKS_QUERY)

    mapping = {}
    for a in result["meta_artworks"]:
        metadata = a.get("metadata", {})
        source_id = metadata.get("source_id")
        if source_id:
            mapping[source_id] = artwork_to_Artwork(a)

    missing = set(source_ids) - set(mapping.keys())
    if missing:
        raise DesignException(
            MISSING_ARTWORKS,
            "Artwork ids provided do not exist",
            f"Artwork ids provided do not exist: {missing}",
        )

    return mapping


def add_new_color(color: CreateColor):
    logger.info(f"add_new_color color={color}")
    artwork_ids = [str(id) for id in color.artwork_ids]

    check_for_missing_artworks(artwork_ids)

    id = res.utils.hash_of(color.code)
    color_artworks = [to_color_artwork(id, a) for a in artwork_ids]

    CREATE_COLOR_MUTATION = """
        mutation upsert_color(
            $color: meta_colors_insert_input!,
            $color_artworks: [meta_colors_artworks_insert_input!]!
        ) {
            insert_meta_colors_one(
                object: $color
                on_conflict: {constraint: colors_pkey, update_columns: [code, name, brand, description]}
            ) {
                id
            }

            insert_meta_colors_artworks(
                objects: $color_artworks
                on_conflict: {constraint: colors_artworks_pkey, update_columns: []}
            ) {
                affected_rows
            }
        }
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute(
        CREATE_COLOR_MUTATION,
        {
            "color": {
                "id": id,
                "code": color.code,
                "name": color.name,
                "brand": color.brand,
                "description": color.description,
            },
            "color_artworks": color_artworks,
        },
    )
    return result["insert_meta_colors_one"]["id"]


def update_color(color: UpdateColor, clear_artworks: bool = True):
    logger.info(f"update_color color={color}")
    id = color.id if color.id is not None else res.utils.hash_of(color.code)

    COLORS_ARTWORKS_MUTATION = None
    COLOR_MUTATION = None

    if color.artwork_ids:
        artwork_ids = [str(id) for id in color.artwork_ids]
        check_for_missing_artworks(artwork_ids)

        color_artworks = [to_color_artwork(id, a) for a in artwork_ids]
        # assume we don't need history
        DELETE_ARTWORKS_MUTATION = f"""
            delete_meta_colors_artworks(where: {{color_id: {{_eq: "{str(id)}"}}}}) {{
                affected_rows
            }}
        """

        INSERT_COLOR_ARTWORKS_MUTATION = f"""
            insert_meta_colors_artworks(
                objects: $color_artworks
                on_conflict: {{constraint: colors_artworks_pkey, update_columns: []}}
            ) {{
                affected_rows
            }}
        """

        COLORS_ARTWORKS_MUTATION = f"""
            {DELETE_ARTWORKS_MUTATION if clear_artworks else ""}
            {INSERT_COLOR_ARTWORKS_MUTATION}
        """

    # even if none of these change a trigger should update the updated_at timestamp
    name_set = f'name: "{color.name}",' if color.name else ""
    brand_set = f'brand: "{color.brand}",' if color.brand else ""
    description_set = (
        f'description: "{color.description}",' if color.description else ""
    )
    COLOR_MUTATION = f"""
        update_meta_colors(
            where: {{
                id: {{_eq: "{str(id)}"}}
            }},
            _set: {{
                {name_set}
                {brand_set}
                {description_set}
                deleted_at: null
            }}
        ) {{
            affected_rows
        }}
    """

    param = (
        "($color_artworks: [meta_colors_artworks_insert_input!]!)"
        if INSERT_COLOR_ARTWORKS_MUTATION
        else None
    )
    UPDATE_COLOR_MUTATION = f"""
        mutation upsert_color {param} {{
            {COLOR_MUTATION if COLOR_MUTATION else ""}
            {COLORS_ARTWORKS_MUTATION if COLORS_ARTWORKS_MUTATION else ""}
        }}
    """
    # logger.debug(UPDATE_COLOR_MUTATION)
    hasura = res.connectors.load("hasura")
    if param:
        result = hasura.execute_with_kwargs(
            UPDATE_COLOR_MUTATION, color_artworks=color_artworks
        )
    else:
        result = hasura.execute(UPDATE_COLOR_MUTATION)
    # logger.debug(result)
    return result


def upsert_color(color: UpdateColor, clear_artworks: bool = False):
    hasura = res.connectors.load("hasura")

    # does it exist?
    id = color.id if color.id is not None else res.utils.hash_of(color.code)
    EXISTING_COLOR_QUERY = """
        query existing_color($id: uuid!) {
            meta_colors(where: {id: {_eq: $id}}) {
                id
            }
        }
    """
    result = hasura.execute(EXISTING_COLOR_QUERY, {"id": str(id)})
    if result["meta_colors"]:
        return update_color(color, clear_artworks=clear_artworks)
    else:
        return add_new_color(color)


def delete_color(id: str, orphan_colors: bool = False):
    logger.info(f"delete_color id={id}, orphan_colors={orphan_colors}")
    now = datetime.datetime.now().isoformat()

    ORPHAN_ARTWORKS_MUTATION = None
    if orphan_colors:
        ORPHAN_ARTWORKS_MUTATION = f"""
            update_meta_colors_artworks(
                where: {{color_id: {{_eq: "{id}"}}}}
                _set: {{deleted_at: "{now}"}}
            ) {{
                affected_rows
            }}
        """

    DELETE_COLOR_MUTATION = f"""
        mutation delete_color($id: uuid!) {{
            {ORPHAN_ARTWORKS_MUTATION if ORPHAN_ARTWORKS_MUTATION else ""}
            update_meta_colors(
                where: {{id: {{_eq: $id}}}}
                _set: {{deleted_at: "{now}"}}
            ) {{
                affected_rows
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute(DELETE_COLOR_MUTATION, {"id": id})
    return result


def get_colors(
    id=None,
    code=None,
    brand=None,
    name=None,
    sign_uris: bool = False,
    limit: int = 10,
    offset: int = 0,
    max_dpi: int = None,
    include_deleted: bool = False,
) -> list[Color]:
    logger.info(f"get_colors name={name} brand={brand} id={id}")

    code_query = f'code: {{_ilike: "{code}"}}' if code is not None else None
    brand_query = f'brand: {{_ilike: "{brand}"}}' if brand is not None else None
    name_query = f'name: {{_ilike: "%{name}%"}}' if name is not None else None

    where = ""
    if id:
        where = f'id: {{_eq: "{id}"}}'
    else:
        where = ", ".join([q for q in [brand_query, name_query, code_query] if q])

    if not include_deleted:
        where = f"deleted_at: {{_is_null: true}}, {where}"

    logger.info(f"where = {where}")
    dpis_fragment = artwork_dpi_query_fragment(sign_uris, max_dpi)
    GET_COLORS_QUERY = f"""
        query getColors {{
            meta_colors(
                where: {{
                    {where}
                }}, 
                order_by: {{created_at: desc}}, 
                limit: {limit},
                offset: {offset}
            ) {{
                id
                code
                name
                brand
                colors_artworks {{
                    artwork {{
                        id
                        name
                        brand
                        original_uri
                        created_at
                        updated_at
                        {dpis_fragment}
                        colors_artworks {{
                            color {{
                                id
                                code
                                name
                                brand
                            }}
                        }}
                    }}
                }}
                created_at
                updated_at
                deleted_at
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute(GET_COLORS_QUERY)
    colors: list[Color] = []
    for c in result["meta_colors"]:
        # colors_artworks = c["colors_artworks"]
        # artworks = [Artwork(**a["artwork"]) for a in colors_artworks]
        artworks = [artwork_to_Artwork(a["artwork"]) for a in c["colors_artworks"]]
        colors.append(Color(**c, artworks=artworks))

    return colors


def get_body_3d_files(
    id: str = None,
    body_code: str = None,
    body_version: str = None,
    use_latest_version: bool = False,
    sign_uris: bool = False,
    limit: int = 10,
    offset: int = 0,
) -> list[Body3DFiles]:
    logger.info(
        f"get_body_3d_files id={id} body_code={body_code} body_version={body_version}"
    )

    where = ""
    if id:
        where = f'id: {{_eq: "{id}"}}'
    else:
        body_code_query = (
            f'body_code: {{_ilike: "%{body_code}%"}}' if body_code is not None else None
        )
        body_version_query = (
            f"version: {{_eq: {body_version}}}"
            if body_version is not None and not use_latest_version
            else None
        )
        where = ", ".join([q for q in [body_code_query, body_version_query] if q])

    where = "metadata: { _contains: { has_3d_model: true } }," + where
    logger.info(f"where = {where}")

    GET_BODY_MODELS_IDS_QUERY = f"""
        query bodies_with_3d {{
            meta_bodies(where: {{ {where} }}
            ) {{
                id
                body_code
                version
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute(GET_BODY_MODELS_IDS_QUERY)
    df = pd.DataFrame(result["meta_bodies"])

    if len(df) == 0:
        return []

    if use_latest_version:
        # get the records with the latest version per body_code
        df = df.sort_values("version", ascending=False).drop_duplicates("body_code")

    body_3d_files: list[Body3DFiles] = []

    body_ids = df["id"].tolist()
    body_ids = ",".join([f'"{b}"' for b in body_ids])
    where = f"id: {{_in: [{body_ids}]}}"

    GET_BODY_MODELS_QUERY = f"""
        query bodies_with_3d {{
            meta_bodies(where: {{ {where} }}
                order_by: {{created_at: desc}},
                limit: {limit},
                offset: {offset}
            ) {{
                body_code
                version
                id
                {"model_3d_uri: signed_model_3d_uri" if sign_uris else "model_3d_uri"}
                {"point_cloud_uri: signed_point_cloud_uri" if sign_uris else "point_cloud_uri"}
            }}
        }}
    """

    result = hasura.execute(GET_BODY_MODELS_QUERY)
    for b in result["meta_bodies"]:
        body_3d_files.append(
            Body3DFiles(
                id=b["id"],
                bodyCode=b["body_code"],
                version=b["version"],
                model3dUri=b["model_3d_uri"],
                pointCloudUri=b["point_cloud_uri"],
            )
        )

    return body_3d_files


# def get_style_list(sku_search: str) -> list[StyleCatalogItem]:
#     hasura = res.connectors.load("hasura")
#     GET_STYLES_QUERY = f"""
#     query getStyleList($s: String!) {{
#         styles(first: 200, search: $s) {{
#             styles {{
#                 code
#             }}
#         }}
#     }}
#     """
#     result = hasura.execute_with_kwargs(GET_STYLES_QUERY, s=sku_search)
#     styles = result.get("styles", {}).get("styles", [])
#     return styles

# airtable = res.connectors.load("airtable")
# styles = airtable["appjmzNPXOuynj6xP"]["tblmszDBvO1MvJrlJ"]
# filters = f'AND(SEARCH("{sku_search}", {{Resonance_code}}))'  # , SEARCH("{body_version}", {{Pattern Version Number}}))'
# print(filters)
# fields = [
#     "Resonance_code",
# ]  # ["_record_id"]
# df = styles.to_dataframe(filters=filters, fields=fields)
# if len(df) > 0:
#     return df.to_dict(orient="records")
# else:
#     return []


def update_latest_apply_color_request_as_dynamic(sizeless_sku):
    airtable = res.connectors.load("airtable")
    apply_color_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
    fields = ["_record_id", "Style Code", "Created At"]
    filters = f"""AND(SEARCH("{sizeless_sku}", {{Style Code}}))"""
    df = apply_color_requests.to_dataframe(fields=fields, filters=filters)
    if len(df) == 0:
        raise DesignException(
            APPLY_COLOR_REQUEST_NOT_FOUND,
            "Apply Color Request Not Found",
            f"Could not find apply color request for sizeless sku {sizeless_sku}",
        )
    latest_id = df.sort_values(by=["Created At"], ascending=False).iloc[0]["_record_id"]

    apply_color_requests.update_record(
        {
            "record_id": latest_id,
            "Color Application Mode": "Automatically",
            "Apply Color Flow Type": "Apply Dynamic Color Workflow",
            # locked: "Requires Turntable Brand Feedback": False
        }
    )

    return latest_id


def get_catalog(
    brand_search: str,
    body_search: str,
    color_search: str,
    limit: int = 10,
    offset: int = 0,
    include_deleted: bool = False,
) -> list[DesignCatalogItem]:
    hasura = res.connectors.load("hasura")

    brand_clause = f'brand: {{_ilike: "%{brand_search}%"}}' if brand_search else None
    body_clause = f'body_code: {{_ilike: "%{body_search}%"}}' if body_search else None
    color_clause = (
        f'color_code: {{_ilike: "%{color_search}%"}}' if color_search else None
    )

    where = ", ".join([q for q in [brand_clause, body_clause, color_clause] if q])

    if not include_deleted:
        where = f"deleted_at: {{_is_null: true}}, {where}"

    GET_DESIGNS = f"""
        query getDesigns {{
            meta_designs(
                where: {{ {where} }}
                limit: {limit}
                offset: {offset}
                order_by: {{created_at: desc}}
            ) {{
                id
                body_code
                body_version
                color_code
            }}
        }}
    """

    result = hasura.execute(GET_DESIGNS)
    if len(result["meta_designs"]) == 0:
        return []

    return [DesignCatalogItem(**d) for d in result["meta_designs"]]


def from_feature(geojson):
    if not geojson:
        return None

    return GeoJson(
        features=None,
        type=geojson["type"],
        geometry=geojson["geometry"],
    )


def from_multifeature(geojson):
    return GeoJson(
        features=[from_feature(feature) for feature in geojson.get("features", [])],
    )


def _get_base_size_from_airtable(body_code: str, body_version: int):
    try:
        airtable = res.connectors.load("airtable")
        bodies = airtable["appa7Sw0ML47cA8D1"]["tblXXuR9kBZvbRqoU"]
        filters = f'AND(SEARCH("{body_code}", {{Body Number}}))'  # , SEARCH("{body_version}", {{Pattern Version Number}}))'
        print(filters)
        fields = [
            "Body",
            "Pattern Version Number",
            "__base_pattern_size",
        ]  # ["_record_id"]
        df = bodies.to_dataframe(filters=filters, fields=fields)
        if len(df) > 0:
            df = df[df["Pattern Version Number"] == body_version]
            if len(df) > 0:
                return df["__base_pattern_size"][0]
    except:
        pass
    return None


# the _accountingsku for the size codes isn't guaranteed to be alphabetically sorted
# e.g. ZZZ00, ZZZZ0, Z1001, ZZZ02 so get the __sortorder from airtable
def _ordered_sizes(sizes):
    sizes_df = None
    try:
        airtable = res.connectors.load("airtable")
        sizes_table = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
        sizes_df = sizes_table.to_dataframe(
            fields=["__record_id", "_accountingsku", "__sortorder"]
        )

        sizes_df = sizes_df[sizes_df["_accountingsku"].isin(sizes)]
        sizes_df = sizes_df.sort_values(by=["__sortorder"])
        sizes = sizes_df["_accountingsku"].tolist()
        return sizes
    except:
        return sorted(sizes)  # otherwise it's a good approximation


class LegacyDesign:
    def __init__(self, style_id):
        self.style_id = style_id
        (
            self.body_code,
            self.body_version,
            self.default_artwork,
            self.placements,
            self.mapping,
            self.artworks,
            self.is_directional,
        ) = self.get_legacy_design(style_id)

    @staticmethod
    def get_legacy_design(style_id):
        hasura = res.connectors.load("hasura")

        GET_STYLE_QUERY = f"""
            query getStyle {{
                style(id: "{style_id}") {{
                    id
                    applyColorFlowType
                    printType
                    bodyVersion
                    body {{
                        code
                    }}
                    artworkFile {{
                        id
                        file {{
                            s3 {{
                                bucket
                                key
                            }}
                        }}
                    }}
                    stylePieces {{
                        artwork {{
                            id
                            file {{
                                s3 {{
                                    bucket
                                    key
                                }}
                            }}
                        }}
                        bodyPiece {{
                            code
                        }}
                    }}
                }}
            }}
        """
        result = hasura.execute(GET_STYLE_QUERY)
        style = result.get("style")
        if not style:
            raise DesignException(
                SKU_NOT_FOUND,
                "SKU Not Found",
                f"Could not find style with id {style_id}",
            )

        body_code = style["body"]["code"]
        body_version = style["bodyVersion"]
        default_artwork_id = style["artworkFile"]["id"]
        is_directional = style["printType"] == "DIRECTIONAL"

        placements = {}
        source_ids = set([default_artwork_id])
        for style_piece in style["stylePieces"]:
            body_piece = style_piece["bodyPiece"]["code"]
            artwork_id = style_piece["artwork"]["id"]
            source_ids.add(style_piece["artwork"]["id"])
            # artwork =  #to_s3_uri(style_piece["artwork"]["file"]["s3"])
            placements[body_piece] = (placements.get(body_piece) or []) + [artwork_id]

        mapping = get_artworks_source_id_mapping(list(source_ids))
        mapping["default"] = mapping[default_artwork_id]

        artworks = {str(a.id): a for a in mapping.values()}

        placements = {
            piece_key: [
                Placement(
                    id=res.utils.uuid_str_from_dict(
                        {"piece": piece_key, "artwork": v, "index": i}
                    ),
                    zIndex=i,
                    tiled=True,
                    pieceName=_remove_body_and_version_from_piece_name(piece_key),
                    artworkId=str(mapping[v].id),
                    pin=Pin(),
                )
            ]
            for piece_key, v in placements.items()
            for i, v in enumerate(v)
        }

        return (
            body_code,
            body_version,
            default_artwork_id,
            placements,
            mapping,
            artworks,
            is_directional,
        )

    def __str__(self):
        return f"{self.style_id}\n{self.body_code}\n{self.body_version}\n{self.default_artwork}\n{self.placements}\n{self.mapping}"

    def __repr__(self):
        return f"LegacyDesign({self.style_id})"


def get_system_placeholders():
    """
    Placeholders are a way of handling dynamic data in the system.

    They can be text or images, and can happen at different stages of the process. For example,
    the size alias happens at the DxA stage, and we can bake this in to the piece image. One
    number is only known at the order stage, so we can load up the piece image.

    We can also have user defined placeholders, which allow them e.g. to provide a mapping of keys
    to images that depend on some variable:

        variable = "star_sign"
        mapping = { "taurus": "<taurus_artwork_id>", "gemini": "<gemini_artwork_id>"}

    The customer will be given a dropdown called star_sign which has 2 options: taurus or gemini.
    When ordered, we will draw the right images in the right places. Could have a key to disinguish between
    different placements, e.g. "front" or "back", pick taurus as your star_sign and one taurus image
    is drawn on the front, and a different taurus image on the back.
    """
    placeholders = {}

    # available at DxA
    placeholders["size_alias"] = Placeholder(
        name="Size",
        variable="size_alias",
        stage="DxA",
        type="text",
        examples=["S", "M", "L", "XL"],
    )

    placeholders["size_code"] = Placeholder(
        name="Size code",
        variable="size_code",
        stage="DxA",
        type="text",
        examples=["3ZZMD", "4ZZLG"],
    )

    # available at Order
    placeholders["one_number"] = Placeholder(
        name="One number",
        variable="one_number",
        description="The unique number of the order",
        stage="Order",
        type="text",
        examples=["10349753", "10313373"],
    )

    # # example of custom placeholders generated on client
    # placeholders["star_sign_logo"] = Placeholder(
    #     name="Small logo of star sign on front left chest",
    #     key="star_sign_logo", # key is the unique identifier for this placeholder
    #     variable="star_sign",
    #     stage="Order",
    #     type="image",
    #     mapping={"taurus": "<taurus_logo_artwork_id>", "gemini": "<gemini_logo_artwork_id>"},
    # )

    # placeholders["star_sign_main"] = Placeholder(
    #     name="Large image of star sign on back",
    #     key="star_sign_main", # key is the unique identifier for this placeholder
    #     variable="star_sign", # same variable name, but different mapping
    #     stage="Order",
    #     type="image",
    #     mapping={"taurus": "<taurus_artwork_id>", "gemini": "<gemini_artwork_id>"},
    # )

    # other examples of Order time placeholders are make instance (how many times SKU ordered),
    # customer name, style instance

    return placeholders


def replace_variables(text, placeholders, design_data: DesignData):
    """
    Replace placeholders in a string with their values
    """
    # temp for now
    replacements = {
        "size_code": design_data.size_code,
    }
    for k, v in placeholders.items():
        text = text.replace(f"{{{k}}}", v)
    return text


def get_design(
    design_id: str = None,
    body_id: str = None,
    body_code: str = None,
    body_version: str = None,
    color_code: str = None,
    max_dpi: int = None,
) -> DesignData:
    logger.info("*** get_design")
    hasura = res.connectors.load("hasura")
    s3 = res.connectors.load("s3")

    artworks = {}
    fonts = {}
    design_obj = {}

    legacy_design = None
    if (design_id or "").startswith("rec"):
        logger.info(f"retreiving legacy design with id {design_id}")
        legacy_design = LegacyDesign(design_id)

        body_code = body_code or legacy_design.body_code
        body_version = body_version or legacy_design.body_version
        artworks = legacy_design.artworks
    # if there's an id or color_code, it's been saved previously so we can get artworks/placements etc
    elif design_id or color_code:
        logger.info(f"retreiving design with id {design_id} or color_code {color_code}")

        # the design id is the easiest, but if not use the other vars
        if design_id:
            where = 'id: {_eq: "' + design_id + '"}'
        else:
            body_id_query = 'body_id: {_eq: "' + body_id + '"}' if body_id else None
            color_code_query = (
                f'color_code: {{_ilike: "{color_code}"}}' if color_code else None
            )
            body_code_query = (
                f'body_code: {{_ilike: "{body_code}"}}'
                if body_code is not None
                else None
            )
            body_version_query = (
                f"version: {{_eq: {body_version}}}"
                if body_version is not None
                else None
            )
            where = ", ".join(
                [
                    q
                    for q in [
                        body_id_query,
                        body_code_query,
                        body_version_query,
                        color_code_query,
                    ]
                    if q
                ]
            )

        GET_DESIGN = f"""
            query getDesign {{
                meta_designs(
                    where: {{
                        {where}
                    }}
                    order_by: {{created_at: desc}}
                    limit: 1
                ) {{
                    id
                    body_id
                    body_code
                    body_version
                    color_code
                    design
                }}
            }}
        """

        result = hasura.execute(GET_DESIGN)
        if len(result["meta_designs"]) == 0:
            raise DesignException(
                DESIGN_NOT_FOUND,
                "Could not find design",
                f"No design found with query {where}",
            )

        design = result["meta_designs"][0]
        logger.info("found design")

        # double check the request against the returned design
        if (
            (design_id and design["id"] != design_id)
            or (body_id and design["body_id"] != body_id)
            or (body_code and design["body_code"] != body_code)
            or (body_version and design["body_version"] != body_version)
            or (color_code and design["color_code"] != color_code)
        ):
            raise DesignException(
                DESIGN_NOT_FOUND,
                "Could not find design matching request",
                f"Conflicting design found with query {where}",
            )

        design_obj = design["design"]
        artwork_ids = set()
        font_ids = set()
        for placements_by_size in design_obj["placements"].values():
            for json_placements in placements_by_size.values():
                for p in json_placements:
                    artwork_ids.add(p["artworkId"])
                    font_ids.add((p.get("textBox", {}) or {}).get("fontId"))
        # remove any blank ones (might just be a text placement)
        blanks = {"", None}
        artwork_ids = [a for a in artwork_ids if a not in blanks]
        font_ids = [f for f in font_ids if f not in blanks]

        artworks = get_artworks_by_ids(list(artwork_ids), max_dpi)
        fonts = get_fonts_by_ids(list(font_ids))

        body_id = design["body_id"]

        # TODO: fill these in too
        # placementPairings = {}
        # annotations = {}
        # gridCoords = {}
        # pointAnchorPairs = {}
    else:
        logger.info("No design, will return blank")

    # get the base body geometry info
    where = ""
    if body_id:
        where = f'id: {{_eq: "{body_id}"}}'
    else:
        body_code_query = (
            f'body_code: {{_ilike: "%{body_code}%"}}' if body_code is not None else None
        )
        body_version_query = (
            f"version: {{_eq: {body_version}}}" if body_version is not None else None
        )
        where = ", ".join([q for q in [body_code_query, body_version_query] if q])

    logger.info(f"getting body with query {where}")
    GET_BODY_DATA_QUERY = f"""
        query bodies_with_3d {{
            meta_bodies(where: {{ {where} }}
                order_by: {{version: desc, created_at: desc}},
                limit: 1,
            ) {{
                body_code
                version
                id
                metadata
                model_3d_uri: signed_model_3d_uri
                point_cloud_uri: signed_point_cloud_uri
                body_pieces(where: {{ 
                    type: {{ _in: ["self", "block_fuse", "lining", "combo"]}}
                }} ) {{
                    piece_key
                    size_code
                    vs_size_code
                    # metadata # "is_base_size": is unreliable until bug fixed and all reprocessed
                    inner_geojson
                    # outer_geojson
                    outer_notches_geojson
                    inner_edges_geojson
                    internal_lines_geojson
                    # buttons_geojson
                }}
            }}
        }}
    """
    # http://localhost:5001/meta-one/design/placements?body_id=d1095375-05c4-db99-857b-b2ab3509acc5
    body_result = hasura.execute(GET_BODY_DATA_QUERY)

    if len(body_result["meta_bodies"]) == 0:
        raise DesignException(
            BODY_NOT_FOUND,
            "Could not find body",
            f"No body found with query {where}",
        )

    logger.info(f"got body ")

    body = body_result["meta_bodies"][0]
    body_code = body["body_code"]
    body_version = body["version"]
    body_id = body["id"]

    piece_size_geojson = {}
    base_size_name = body.get("metadata", {}).get("base_size_name")
    if base_size_name is None:
        logger.info(f"getting base size from airtable for {body_code} v{body_version}")
        base_size_name = _get_base_size_from_airtable(body_code, int(body_version))

    logger.info("processing body pieces")
    base_size_code = None
    sizes = set()
    alias_size_mapping = {}
    for piece in body["body_pieces"]:
        piece_name = _remove_body_and_version_from_piece_name(piece["piece_key"])

        # if it's not there already add a blank dict
        if piece_name not in piece_size_geojson:
            piece_size_geojson[piece_name] = {}

        piece_geojsons = PieceGeoJsons(
            pieceName=piece_name,
            sizeCode=piece["size_code"],
            innerGeoJson=from_feature(piece["inner_geojson"]),
            edgesGeoJson=from_multifeature(piece["inner_edges_geojson"]),
            notchesGeoJson=from_feature(piece["outer_notches_geojson"]),
            internalLinesGeoJson=from_multifeature(piece["internal_lines_geojson"]),
            isBaseSize=piece["vs_size_code"] == base_size_name,
        )

        piece_size_geojson[piece_name][piece["size_code"]] = piece_geojsons

        if base_size_code is None and piece["vs_size_code"] == base_size_name:
            base_size_code = piece["size_code"]
        sizes.add(piece["size_code"])
        alias_size_mapping[piece["vs_size_code"]] = piece["size_code"]

    # if there are other sizes with 3d models, return those too
    logger.info("check for 3d models")
    sizes_with_3d_model = body.get("metadata", {}).get("sizes_with_3d_model", [])
    body_3d_uris = {}
    if sizes_with_3d_model:
        body_code_lower = body_code.lower().replace("-", "_")
        base_3d_uri = "s3://meta-one-assets-prod/bodies/3d_body_files"
        base_3d_uri = (
            f"{base_3d_uri}/{body_code_lower}/v{body_version}/extracted/exports_3d"
        )
        expiry = 60 * 60 * 24
        # for each size, check the files exist, then get a presigned url
        for size in sizes_with_3d_model:
            size_3d_model_uri = f"{base_3d_uri}/{size}/3d.glb"
            size_point_cloud_uri = f"{base_3d_uri}/{size}/point_cloud.json"

            if s3.exists(size_3d_model_uri) and s3.exists(size_point_cloud_uri):
                unaliased = alias_size_mapping.get(size, size)
                body_3d_uris[unaliased] = Body3DFiles(
                    id=body_id,
                    bodyCode=body_code,
                    version=body_version,
                    model3dUri=s3.generate_presigned_url(size_3d_model_uri, expiry),
                    pointCloudUri=s3.generate_presigned_url(
                        size_point_cloud_uri, expiry
                    ),
                )

    # it seems sometimes the airtable base size code can change for the body, rectify if we can
    if base_size_code is None:
        logger.info("getting sizes from aliases")
        base_size_code = get_sizes_from_aliases(base_size_name)[0]["code"]

    if not body_3d_uris:
        body_3d_uris[base_size_code] = Body3DFiles(
            id=body_id,
            bodyCode=body_code,
            version=body_version,
            model3dUri=body["model_3d_uri"],
            pointCloudUri=body["point_cloud_uri"],
        )

    # the _accountingsku for the size codes isn't guaranteed to be alphabetically sorted
    # e.g. ZZZ00, ZZZZ0, Z1001, ZZZ02 so get the __sortorder from airtable
    logger.info("getting ordered sizes")
    sizes = _ordered_sizes(sizes)

    if legacy_design:
        if legacy_design.is_directional:
            default_artwork_id = str(legacy_design.mapping["default"].id)
            base_placement_id = res.utils.uuid_str_from_dict(
                {
                    "style_id": legacy_design.style_id,
                    "artwork": "default",
                }
            )
            design_obj["basePlacement"] = Placement(
                id=base_placement_id,
                zIndex=0,
                tiled=True,
                pieceName="base",  # TODO: work out base placement stuff
                artworkId=default_artwork_id,
            )
            directional_placements = {}
            for piece in body["body_pieces"]:
                piece_name = _remove_body_and_version_from_piece_name(
                    piece["piece_key"]
                )
                id = res.utils.uuid_str_from_dict(
                    {
                        "style_id": legacy_design.style_id,
                        "piece": piece_name,
                    }
                )
                directional_placements[piece_name] = {
                    s: [
                        Placement(
                            id=id,
                            zIndex=0,
                            tiled=True,
                            pieceName=piece_name,  # TODO: work out base placement stuff
                            artworkId=default_artwork_id,
                        )
                    ]
                    for s in sizes
                }

            design_obj["placements"] = directional_placements
        else:
            blank_placements = {
                _remove_body_and_version_from_piece_name(piece["piece_key"]): {
                    s: [] for s in sizes
                }
                for piece in body["body_pieces"]
            }
            placements_by_size = {
                k: {s: v for s in sizes} for k, v in legacy_design.placements.items()
            }
            design_obj["placements"] = {**blank_placements, **placements_by_size}

    placeholders = get_system_placeholders()

    def add_mapping(key, mapping):
        placeholders[key] = Placeholder(
            **{
                **placeholders[key].dict(),
                "mapping": mapping,
            },
        )

    add_mapping("size_alias", {v: k for k, v in alias_size_mapping.items()})
    add_mapping("size_code", {k: k for k in alias_size_mapping.values()})

    result = DesignData(
        modelUri=body["model_3d_uri"],
        pointCloudUri=body["point_cloud_uri"],
        artworks=artworks,
        fonts=fonts,
        basePlacement=design_obj.get("basePlacement"),
        baseSizeCode=base_size_code,
        geoJsons=piece_size_geojson,
        placements=design_obj.get("placements", {}),
        placementPairings=design_obj.get("placementPairings", {}),
        annotations=design_obj.get("annotations", {}),
        gridCoords=design_obj.get("gridCoords", {}),
        pointAnchorPairs=design_obj.get("pointAnchorPairs", {}),
        guideLines=design_obj.get("guideLines", []),
        sizes=sizes,
        bodyCode=body["body_code"],
        bodyVersion=body["version"],
        brandCode=body["body_code"].split("-")[0],
        bodyId=body["id"],
        body3dUris=body_3d_uris,
        aliasSizeMapping=alias_size_mapping,
        placeholders={**(design_obj.get("placeholders") or {}), **placeholders},
    )

    return result


def artwork_ids_for_placements(placements):
    artwork_ids = set()
    for pieces in placements.values():
        for placements in pieces.values():
            artwork_ids.update([placement.artworkId for placement in placements])

    # remove any blank ones
    artwork_ids.discard("")
    artwork_ids.discard(None)

    return list(artwork_ids)


def design_exists(design_id: str):
    hasura = res.connectors.load("hasura")

    exists = hasura.execute_with_kwargs(
        """
        query get_design($design_id: uuid!) {
            meta_designs_by_pk(id: $design_id) {
                id
            }
        }
    """,
        design_id=design_id,
    )
    return exists["meta_designs_by_pk"] is not None


def sku_exists(sku: str):
    hasura = res.connectors.load("hasura")
    c1api_query = """
    query SkuExists($sku: String!) {
        style(code: $sku) {
            code
        }
    }
    """
    c1api_result = hasura.execute_with_kwargs(
        c1api_query,
        sku=sku,
    )
    return c1api_result["style"] is not None


def assert_editor_type(editor_type: str):
    if editor_type not in [
        apply_dynamic_color.STYLE_EDITOR_TYPE_3D,
        apply_dynamic_color.STYLE_EDITOR_TYPE_2D,
    ]:
        raise DesignException(
            INVALID_EDITOR_TYPE,
            "Editor type not found",
            f"Invalid editor_type: {editor_type}",
        )


def download_s3_font(font_id: str):
    hasura = res.connectors.load("hasura")
    s3 = res.connectors.load("s3")

    result = hasura.execute_with_kwargs(
        """
        query get_font($font_id: uuid!) {
            meta_fonts_by_pk(id: $font_id) {
                id
                name
                uri
            }
        }
    """,
        font_id=font_id,
    )
    font = result["meta_fonts_by_pk"]

    if font:
        uri = font["uri"]
        (name, ext) = url_to_filename_and_ext(uri)
        name = font.get("name") or name
        target = f"/tmp/{font_id}.{ext}"
        if not os.path.exists(target):
            logger.info(f"Downloading font {name}...")
            s3._download(uri, "/tmp", f"{font_id}.{ext}")
        else:
            logger.info(f"Using cached font {name}...")

        return {"path": target, "name": name}

    return None


def has_glyph(font, glyph):
    for table in font["cmap"].tables:
        if ord(glyph) in table.cmap.keys():
            return True
    return False


def check_texts_for_unsupported_characters(design: UserDesign):
    unsupported_text = []
    font_cache = {}
    try:
        for placements in design.placements.values():
            # no need to check for every size, just the first one
            first_placements = next(iter(placements.values()))
            for placement in first_placements:
                if placement.textBox:
                    text = placement.textBox.text
                    font_id = placement.textBox.fontId
                    if font_id not in font_cache:
                        font_details = download_s3_font(font_id)
                        font_path = font_details.get("path")
                        font_cache[font_id] = font_details
                    else:
                        font_details = font_cache[font_id]
                        font_path = font_details.get("path")

                    if not font_path:
                        raise DesignException(
                            MISSING_FONTS,
                            "Font not found",
                            f"Font not found for id: {font_id}",
                        )
                    font = TTFont(font_path)
                    # TODO: replace the text properly as the replacements might have unsupported chars
                    text = re.sub(r"\{.*?\}", "", text)
                    unsupported = [c for c in text if not has_glyph(font, c)]
                    if unsupported:
                        unsupported_text.append(
                            {
                                "text": text,
                                "unsupported": "".join(list(set(unsupported))),
                                "font_name": font_details.get("name"),
                            }
                        )
        # clean up the font cache
        for font_details in font_cache.values():
            os.remove(font_details.get("path"))
    except Exception as e:
        logger.error(f"Error checking for unsupported characters: {e}")

    if unsupported_text:
        summary = "\n".join(
            [
                f"""Text: "{t['text']}", Unsupported chars: {t['unsupported']}, Font: "{t['font_name']}" """
                for t in unsupported_text
            ]
        )

        raise DesignException(
            UNSUPPORTED_CHARACTERS,
            "Unsupported characters",
            f"Unsupported characters in text: {summary}",
        )


def s3_uri_from_bucket_key(file):
    return f"s3://{file['s3']['bucket']}/{file['s3']['key']}"


def get_existing_color(color_code):
    gql = ResGraphQLClient()

    Q = """
        query GetColor($code: String!) {
            color(code: $code) {
                id
                code
                isTrashed
                activeArtworkFile {id, file {s3 {bucket, key}}}
                artworkFiles {id, file {s3 {bucket, key}}}
            }
        }
    """

    result = gql.query(Q, {"code": color_code})

    return result["data"]["color"] if result["data"] else None


def get_c1_s3_image_location(name):
    gql = ResGraphQLClient()

    result = gql.query(
        """
    query blah {
        upload {
            s3 {
                location(contentType: "image/png", extension: "png", name: "jltest") {
                    bucket
                    key
                    url
                }
            }
        }
    }""",
        {"name": name},
    )
    result = result["data"]["upload"]["s3"]["location"]

    return result


def create_c1_file(bucket, key, image_name, size):
    gql = ResGraphQLClient()

    result = gql.query(
        """
        mutation CreateS3File($createS3FileInput2: CreateS3FileInput!) {
            createS3File(input: $createS3FileInput2) {
                file {
                    id
                    name
                    size
                    s3 {
                        bucket
                        key
                    }
                    type
                    url
                }
            }
        }
        """,
        {
            "createS3FileInput2": {
                "bucket": bucket,
                "key": key,
                "name": image_name,
                "size": size,
                "type": "image/png",
            }
        },
    )

    return result["data"]["createS3File"]["file"]


# def get_next_color_code(color_code):
#     base = 26
#     result = []
#     carry = 1

#     for char in reversed(color_code):
#         value = ord(char) - ord("A")  # Convert character to base 26 value
#         value += carry
#         carry = value // base
#         result.append(chr((value % base) + ord("A")))  # Convert back to character

#     if carry:
#         result.append("A")  # Add an extra 'A' if there's a carry

#     return "".join(reversed(result))


# def get_unique_color_code(brand_code, color_code):
#     # use regex to check color_code is 6 alphabetic characters and uppercase
#     # if it matches assume it's an upsert or something and use that
#     if re.match(r"^[A-Z]{6}$", color_code):
#         return color_code

#     # otherwise make it unique, usually first 4 chars of color/style name
#     # then 2 random ones
#     ignored_words = [
#         "the",
#         "and",
#         "of",
#         "for",
#         "in",
#         "on",
#         "with",
#         "by",
#         "at",
#         "to",
#         "from",
#         "as",
#         "an",
#         "a",
#     ]
#     new_color_code = "".join(
#         [w[0].upper() for w in color_code.split() if w.lower() not in ignored_words]
#     )[:4]

#     if new_color_code == "":
#         new_color_code = brand_code or "__"

#     next_color_code = "ZZZZZZ"
#       gql = ResGraphQLClient()


#     while len(new_color_code) > 1 and not next_color_code.startswith(next_color_code):
#         # get all the colors that look like this and pick the next one
#         matches = gql.query(
#             """
#             query colors($code: String!) {
#                 colors(first:1000, where: {code: {match: $code}}) {
#                     colors {
#                     id
#                     code
#                     }
#             }
#         """,
#             {"code": f"^{new_color_code}"},
#         )
#         top = sorted(matches["data"]["colors"]["colors"], key=lambda x: x["code"])[-1]
#         next_color_code = get_next_color_code(top["code"])

#         # knock one off new_color_code in case we've rolled over e.g. looking for ABCD
#         # but the next one after ABCDZZ is ABCEAA, we'll need to check ABC for the next one
#         new_color_code = new_color_code[:-1]

#     if not len(new_color_code) > 1:
#         raise Exception("Couldn't find a unique color code")

#     return next_color_code


def upload_artwork_on_c1(brand_code, image_uri, image_name):
    s3 = res.connectors.load("s3")

    # get an upload location from gql
    bucket_key = get_c1_s3_image_location(image_name)
    bucket = bucket_key["bucket"]
    key = bucket_key["key"]
    s3_uri = f"s3://{bucket}/{key}"

    # upload it to c1's preferred location
    s3_uri = sync_artworks.upload_external_uri_to_s3(image_uri, brand_code, s3_uri)
    if s3_uri is None:
        raise DesignException(
            BAD_PARAMETERS,
            "Invalid image uri",
            f"Could not download image from {image_uri}",
        )

    # create a c1 "file"
    size = next(s3.get_files_sizes([s3_uri]))["size"]
    c1_file = create_c1_file(bucket, key, image_name, size)

    return c1_file


def upsert_color_on_c1(existing_color, brand_code, color_code, image_uri, image_name):
    gql = ResGraphQLClient()

    # create a c1 "file" from the artwork
    c1_file = upload_artwork_on_c1(brand_code, image_uri, image_name)

    if existing_color:
        # remove any existing artwork (assuming one for now)
        if existing_color["activeArtworkFile"]:
            gql.query(
                """
                mutation RemoveArtworkFileFromColor($id: ID!, $input: RemoveArtworkFileFromColorInput) {
                    removeArtworkFileFromColor(id: $id, input: $input) {
                        color {
                            artworkFileIds
                        }
                    }
                }""",
                {
                    "id": existing_color["id"],
                    "input": {
                        "artworkFileId": existing_color["activeArtworkFile"]["id"]
                    },
                },
            )

        # add the new artwork
        gql.query(
            """
            mutation AddArtworkFileToColor($id: ID!, $input: AddArtworkFileToColorInput) {
                addArtworkFileToColor(id: $id, input: $input) {
                    color {
                        artworkFileIds
                    }
                }
            }""",
            {
                "id": existing_color["id"],
                "input": {
                    "fileId": c1_file["id"],
                    "isApprovedForAllMaterials": True,
                    "name": image_name,
                },
            },
        )

        return existing_color
    else:

        # get a unique code for the color
        # c1 does this for you color_code = get_unique_color_code(brand_code, color_code)

        # create it
        brand_id = gql.query(
            """query($c: String!) { brand(code: $c) {id, name}}""", {"c": "JD"}
        )
        brand_id = brand_id["data"]["brand"]["id"]
        result = gql.query(
            """
            mutation CreateColor($createColorInput: CreateColorInput!) {
                createColor(input: $createColorInput) {
                    color {
                        code
                        id
                    }
                }
            }""",
            {
                "createColorInput": {
                    "brandId": brand_id,
                    # pad color with As so it's at least 4 chars, this will return new color_code
                    "name": color_code.ljust(4, "A"),
                    "artworkFile": {"fileId": c1_file["id"], "name": image_name},
                    "image": {
                        "fileId": c1_file["id"],
                    },
                }
            },
        )

        return result["data"]["createColor"]["color"]


def copy_body_glb_to_style(body_code, body_version, color_code):
    s3 = res.connectors.load("s3")

    source = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_code.lower().replace('-', '_')}/v{body_version}/extracted"
    dest = f"s3://meta-one-assets-prod/color_on_shape/{body_code.lower().replace('-', '_')}/v{body_version}/{color_code.lower().replace('-', '_')}"

    for f in ["3d.glb", "point_cloud.json"]:
        source_uri = f"{source}/{f}"
        dest_uri = f"{dest}/{f}"
        if not s3.exists(dest_uri):
            s3.copy(source_uri, dest_uri)


def create_directional_via_c1(
    body_code: str,
    material_code: str,
    color_code: str,
    body_version: int,
    brand_code: str,
    image_uri: str,
    image_name: str,
    c1_action: str = None,  # "preexist", "upsert", "ignore"
):
    gql = ResGraphQLClient()

    # TODO: no idea why body_version isn't needed, maybe createApplyColor just gets latest...
    sku = f"{body_code} {material_code} {color_code}"

    if material_code is None or material_code == "":
        body = gql.query(
            body_graphql_queries.GET_BODY,
            placeholders={"number": body_code},
        )["data"]["body"]

        if len(body["onboardingMaterials"]) == 0:
            raise DesignException(
                BAD_PARAMETERS,
                "No onboarding materials",
                f"No onboarding materials found for body: {body_code}",
            )

        material_code = body["onboardingMaterials"][0]["code"]

    c1_action = c1_action or "ignore"

    existing_color = get_existing_color(color_code)
    if c1_action == "preexist" and not existing_color:
        raise DesignException(
            COLOR_NOT_FOUND,
            "Color not found",
            f"Color not found for code: {color_code}",
        )

    if c1_action != "ignore":
        existing_color = upsert_color_on_c1(
            existing_color, brand_code, color_code, image_uri, image_name
        )
        # existing_color = {"code": "AUTOML", "id": "65f0383339c5340008db2499"}
        # create a style and apply color request
        style_result = c1_upsert_style(
            sku,
            brand_code,
            body_code,
            material_code,
            existing_color["id"],
            start_via_airtable=True,
        )
        assign_trims_to_first_one_style(style_result["style_id"])

    color_code = existing_color["code"]

    copy_body_glb_to_style(
        body_code, body_version, color_code
    )  # temp until we get auto glb creation

    sku = f"{body_code} {material_code} {color_code}"

    # I think I'd like to add it to the design too
    create_directional_via_postgres(
        body_code,
        material_code,
        color_code,
        body_version,
        brand_code,
        image_uri,
        image_name,
        c1_action,
    )

    return sku


# terrible name
def create_directional_via_postgres(
    body_code: str,
    material_code: str,
    color_code: str,
    body_version: int,
    brand_code: str,
    image_uri: str,
    image_name: str,
    submit_job: bool = False,
):
    hasura = res.connectors.load("hasura")

    # get a blank design
    design = get_design(body_code=body_code, body_version=body_version)

    # if we don't have the image saved to the db
    GET_ARTWORK_BY_SOURCE_URI = """
        query get_artwork_by_source_uri($uri: String!) {
            meta_artworks(where: {metadata: {_contains: {source_uri: $uri}}}) {
                id
            }
        }
    """
    result = hasura.execute_with_kwargs(GET_ARTWORK_BY_SOURCE_URI, uri=image_uri)
    artwork_id = None
    if len(result["meta_artworks"]) == 0:
        a = CreateArtwork(
            name=image_name or f"{body_code} {material_code} {color_code}",
            brand=brand_code,
            externalUri=image_uri,
        )
        result = add_artwork(a)
        # if the result is a CreateArtworkResponse
        if not result.id:
            raise DesignException(
                BAD_PARAMETERS,
                "Artwork not created",
                f"Artwork not created for uri: {image_uri} {result.errors}",
            )
        artwork_id = result.id
    else:
        artwork_id = result["meta_artworks"][0]["id"]

    # add a placement to every piece with the artwork
    artwork_id = str(artwork_id)
    logger.info("Adding directional placements")
    for piece_name in design.geoJsons.keys():
        design.placements[piece_name] = {}
        for size in design.sizes:
            placement = Placement(
                id=str(
                    res.utils.uuid_str_from_dict(
                        {"piece": piece_name, "artwork": artwork_id, "index": 0}
                    )
                ),
                zIndex=0,
                tiled=True,
                pieceName=piece_name,
                artworkId=artwork_id,
                isDefault=True,
                pin=Pin(),
            )
            design.placements[piece_name][size] = [placement]

    logger.info(design.placements)

    return add_design(
        design=UserDesign(**design.dict()),
        sizeless_sku=f"{body_code} {material_code} {color_code}",
        body_version=body_version,
        brand=brand_code,
        submit_job=submit_job,
        bypass_create_one=True,
    )


def add_design(
    design: UserDesign,
    sizeless_sku: str,
    body_version: int,
    brand: str = None,
    submit_job: bool = True,
    editor_type: str = apply_dynamic_color.STYLE_EDITOR_TYPE_3D,
    bypass_create_one: bool = False,
):
    editor_type = editor_type or apply_dynamic_color.STYLE_EDITOR_TYPE_3D
    assert_editor_type(editor_type)
    hasura = res.connectors.load("hasura")

    # sizeless_sku should be two letters, a dash, 4 numbers, a space, 4-5 alphanumeric, then 6 letters
    import re

    r = re.compile(r"^[A-Z]{2}-\d{4} [A-Z0-9]{4,5} [A-Z]{6}$")
    if not r.match(sizeless_sku):
        raise DesignException(
            BAD_PARAMETERS, "Invalid SKU", f"Invalid SKU: {sizeless_sku}"
        )

    # must provide either body_id or body_code and body_version
    if body_version is None:
        raise Exception("Must provide body_version")

    sku_parts = sizeless_sku.split(" ")

    body_code = sku_parts[0]
    brand_code = brand or body_code.split("-")[0]
    color_code = sku_parts[2]

    # check the sku exists
    if not bypass_create_one and not sku_exists(sizeless_sku):
        raise DesignException(
            SKU_NOT_FOUND, "Could not find SKU", f"Couldn't find SKU: {sizeless_sku}"
        )

    GET_BODY_ID = """
        query get_body_id($body_code: String!, $body_version: numeric!) {
            meta_bodies(where: {body_code: {_eq: $body_code}, version: {_eq: $body_version}}) {
                id
            }
        }
    """
    result = hasura.execute_with_kwargs(
        GET_BODY_ID, body_code=body_code, body_version=body_version
    )
    if len(result["meta_bodies"]) == 0:
        raise DesignException(
            BODY_NOT_FOUND,
            "Could not find body",
            f"Couldn't find body: {body_code} version: {body_version}",
        )

    body_id = result["meta_bodies"][0]["id"]

    design_id = res.utils.uuid_str_from_dict(
        {"body_code": body_code, "body_version": body_version, "color_code": color_code}
    )

    # if this design already exists, automatically do an update
    if design_exists(design_id):
        return update_design(
            design=design,
            design_id=design_id,
            sizeless_sku=sizeless_sku,
            submit_job=submit_job,
            bypass_create_one=bypass_create_one,
        )

    check_texts_for_unsupported_characters(design)

    copy_body_glb_to_style(
        body_code, body_version, color_code
    )  # temp until we get auto glb creation

    # get the artworks from the placements and create a color for it if there isn't one already
    color = UpdateColor(
        id=res.utils.hash_of(color_code),
        brand=brand_code,
        code=color_code,
        artwork_ids=artwork_ids_for_placements(design.placements),
        name=color_code,  # TODO: add in a name
    )

    upsert_color(color)
    # {json.dumps(design.dict())}
    INSERT_DESIGN_QUERY = f"""
        mutation insert_design($design: jsonb!) {{
            insert_meta_designs_one(object: {{
                id: "{design_id}",
                body_id: "{body_id}",
                body_code: "{body_code}",
                body_version: {body_version},
                color_id: "{color.id}",
                color_code: "{color_code}",
                brand: "{brand_code}",
                design: $design
            }}) {{
                id
            }}
        }}
    """
    result = hasura.execute_with_kwargs(INSERT_DESIGN_QUERY, design=design.dict())

    if not bypass_create_one:
        apply_dynamic_color.set_airtable_flow_fields(
            sizeless_sku, editor_type=editor_type
        )

    if submit_job == True:
        rec_id = None
        if not bypass_create_one:
            rec_id = update_latest_apply_color_request_as_dynamic(sizeless_sku)
        apply_dynamic_color.generate_design(
            design_id,
            sizeless_sku,
            body_version,
            apply_color_rec_id=rec_id,
            testing=bypass_create_one,
        )

    return result["insert_meta_designs_one"]["id"]


def update_design(
    design: UserDesign,
    design_id: str,
    sizeless_sku: str,
    submit_job: bool = True,
    editor_type: str = apply_dynamic_color.STYLE_EDITOR_TYPE_3D,
    bypass_create_one: bool = False,
):
    editor_type = editor_type or apply_dynamic_color.STYLE_EDITOR_TYPE_3D
    assert_editor_type(editor_type)
    hasura = res.connectors.load("hasura")

    # check the sku exists
    if not bypass_create_one and not sku_exists(sizeless_sku):
        raise DesignException(
            SKU_NOT_FOUND, "Could not find SKU", f"Couldn't find SKU: {sizeless_sku}"
        )

    GET_COLOR_CODE = """
        query get_color_code($design_id: uuid!) {
            meta_designs_by_pk(id: $design_id) {
                color_code
                brand
                body {
                    version
                }
            }
        }
    """
    result = hasura.execute_with_kwargs(GET_COLOR_CODE, design_id=design_id)
    color_code = result["meta_designs_by_pk"]["color_code"]
    brand_code = result["meta_designs_by_pk"]["brand"]
    body_version = result["meta_designs_by_pk"]["body"]["version"]

    check_texts_for_unsupported_characters(design)

    copy_body_glb_to_style(
        sizeless_sku.split(" ")[0], body_version, color_code
    )  # temp until we get auto glb creation

    # get the artworks from the placements and create a color for it if there isn't one already
    color = UpdateColor(
        id=res.utils.hash_of(color_code),
        brand=brand_code,
        code=color_code,
        artwork_ids=artwork_ids_for_placements(design.placements),
        name=color_code,  # TODO: add in a name
    )

    upsert_color(color)

    UPDATE_DESIGN_QUERY = f"""
        mutation update_design($design: jsonb!) {{
            update_meta_designs_by_pk(pk_columns: {{id: "{design_id}"}}, _set: {{
                design: $design,
                updated_at: "now()"
                deleted_at: null
            }}) {{
                id
            }}
        }}
    """
    result = hasura.execute_with_kwargs(UPDATE_DESIGN_QUERY, design=design.dict())

    if not bypass_create_one:
        apply_dynamic_color.set_airtable_flow_fields(
            sizeless_sku, editor_type=editor_type
        )

    if submit_job == True:
        rec_id = None
        if not bypass_create_one:
            rec_id = update_latest_apply_color_request_as_dynamic(sizeless_sku)
        apply_dynamic_color.generate_design(
            design_id,
            sizeless_sku,
            body_version,
            apply_color_rec_id=rec_id,
            testing=bypass_create_one,
        )

    return result["update_meta_designs_by_pk"]["id"]


def delete_design(
    design_id: str,
):
    now = datetime.datetime.now().isoformat()
    hasura = res.connectors.load("hasura")

    DELETE_DESIGN_QUERY = f"""
        mutation delete_design($design_id: uuid!) {{
            update_meta_designs_by_pk(pk_columns: {{id: $design_id}}, _set: {{
                deleted_at: "{now}"
            }}) {{
                id
            }}
        }}
    """
    result = hasura.execute_with_kwargs(DELETE_DESIGN_QUERY, design_id=design_id)

    return result.get("update_meta_designs_by_pk") != None


def url_to_filename_and_ext(uri, default_ext="ttf"):
    """
    Helper function to get a filename and ext from a uri e.g.
    s3://meta-one-assets-prod/fonts/brand/uuid.ext -> uuid.ext
    http://blah.com/blah.ttf -> blah.ttf
    """

    end_part = uri.split("/")[-1]
    end_part = uri.split("?")[0]
    [filename, ext] = (
        end_part.split(".") if "." in end_part else [end_part, default_ext]
    )

    return filename, ext


def get_fonts(
    id=None,
    search=None,
    brand=None,
    name=None,
    sign_uris=False,
    limit: int = 10,
    offset: int = 0,
    include_deleted: bool = False,
) -> list[Font]:
    logger.info(f"get_fonts name={name} brand={brand} id={id}")

    id_query = f'id: {{_eq: "{id}"}}' if id is not None else None

    brand_query = f'brand: {{_ilike: "{brand}"}}' if brand is not None else None
    name_query = f'name: {{_ilike: "%{name}%"}}' if name is not None else None
    search_query = None
    if search:
        search_query = f"""
            _or: [
                {{brand: {{_ilike: "%{search}%"}}}},
                {{name: {{_ilike: "%{search}%"}}}},
                {{source: {{_ilike: "%{search}%"}}}},
            ]
        """

    other_queries = [q for q in [brand_query, name_query, search_query] if q]
    where = id_query if id_query else ", ".join(other_queries)
    if not include_deleted:
        where = f"deleted_at: {{_is_null: true}}, {where}"

    # logger.info(f"where = {where}")
    GET_FONTS_QUERY = f"""
        query latestArtwork {{
            meta_fonts(where: {{
                {where}
            }}, 
                order_by: {{created_at: desc}}, 
                limit: {limit},
                offset: {offset}
            ) {{
                id
                name
                brand
                uri: {"signed_" if sign_uris else ""}uri
                source
                metadata
                created_at
                updated_at
                deleted_at
            }}
        }}
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute(GET_FONTS_QUERY)
    fonts: list[Font] = [Font(**f) for f in result["meta_fonts"]]

    return fonts


def download_font(source: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
    }
    response = requests.get(source, headers=headers)
    if response.status_code != 200:
        raise DesignException(
            BAD_PARAMETERS,
            "Invalid font source",
            f"Font source is invalid {source}",
        )
    return response.content


def add_font(font: Font):
    # if this font already exists, automatically do an update
    if font.id and get_fonts(id=font.id):
        return update_font(font)

    if not font.source:
        raise DesignException(
            BAD_PARAMETERS,
            "Missing source",
            "Font source is required",
        )

    hasura = res.connectors.load("hasura")
    s3 = res.connectors.load("s3")
    brand = font.brand or "__"
    id = res.utils.uuid_str_from_dict(
        {"source": font.source, "name": font.name, "brand": brand}
    )

    (_, ext) = url_to_filename_and_ext(font.source)
    s3_dest_uri = f"s3://meta-one-assets-prod/fonts/{brand}/{id}.{ext}"
    if font.source.startswith("s3://"):
        s3.copy(font.source, s3_dest_uri)
    else:
        # download the font and save to s3
        content = download_font(font.source)
        s3.save(s3_dest_uri, content)

    # could put metadata in like is it serif etc.
    font = {**font.dict(), "id": id, "uri": s3_dest_uri}
    # remove any Nones
    font = {k: v for k, v in font.items() if v is not None}
    INSERT_FONT_QUERY = f"""
        mutation insert_font($font: meta_fonts_insert_input!) {{
            insert_meta_fonts_one(object: $font) {{
                id
            }}
        }}
    """
    result = hasura.execute_with_kwargs(INSERT_FONT_QUERY, font=font)

    return result["insert_meta_fonts_one"]["id"]


def update_font(font: Font):
    hasura = res.connectors.load("hasura")

    GET_FONT_QUERY = f"""
        query get_font($font_id: uuid!) {{
            meta_fonts_by_pk(id: $font_id) {{
                id
                name
                brand
                metadata
                deleted_at
            }}
        }}
    """
    result = hasura.execute_with_kwargs(GET_FONT_QUERY, font_id=str(font.id))
    original_font = result["meta_fonts_by_pk"]

    uri = font.uri
    source = font.source
    if font.source:
        s3 = res.connectors.load("s3")
        (_, ext) = url_to_filename_and_ext(font.source)
        uri = f"s3://meta-one-assets-prod/fonts/{font.brand}/{font.id}.{ext}"

        if font.source.startswith("s3://"):
            # unlikely but if we're given an external s3 this might break
            s3.copy(font.source, uri)
        else:
            # download the font and save to s3
            content = download_font(font.source)
            s3.save(uri, content)

    font = {**original_font, **font.dict(), "uri": uri, "source": source}
    # remove any Nones
    font = {k: v for k, v in font.items() if v is not None}
    font["deleted_at"] = None
    font["id"] = str(font["id"])
    UPDATE_FONT_QUERY = f"""
        mutation update_font($font: meta_fonts_set_input!) {{
            update_meta_fonts_by_pk(pk_columns: {{id: "{font["id"]}"}}, _set: $font) {{
                id
            }}
        }}
    """
    result = hasura.execute_with_kwargs(UPDATE_FONT_QUERY, font=font)

    return result["update_meta_fonts_by_pk"]["id"]


def delete_font(font_id: str):
    now = datetime.datetime.now().isoformat()
    hasura = res.connectors.load("hasura")

    DELETE_FONT_QUERY = f"""
        mutation delete_font($font_id: uuid!) {{
            update_meta_fonts_by_pk(pk_columns: {{id: $font_id}}, _set: {{
                deleted_at: "{now}"
            }}) {{
                id
            }}
        }}
    """
    result = hasura.execute_with_kwargs(DELETE_FONT_QUERY, font_id=font_id)

    return result.get("update_meta_fonts_by_pk") != None
