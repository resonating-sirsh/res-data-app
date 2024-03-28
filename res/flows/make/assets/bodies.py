import res
from ast import literal_eval
import pandas as pd

#### sample one -specs that can be queried
#  TK-6093-V5-00001, CC-6001-V8-00001,
###


def query_one_specification_by_key(
    provider=None,
    body="tk-6077",
    version="v6",
    res_sell_size="0zzxx",
    expand_levels={"make.one_specification": 1, "meta.body_pieces": 1},
    column_mapping=None,
):
    """
    Example key: TK-6077-V6-0ZZXX-00001
    """
    # TODO:  add size to the spec key on the way in and way out
    key = f"{body.upper()}-{version.upper()}-{res_sell_size.upper()}-00001"

    res.utils.logger.debug(f"Making a request for one specification {key}")

    provider = provider or res.connectors.load("dgraph")

    q = f"""{{
      get(func: type(make.body_pieces))
      @filter(eq(one_specification_key, {key}) )
      {{
        uid
        key
        body_piece_key
        color_code
        material_code
        one_specification_key
        make.one_specification{{
          key
          body_key
          meta.bodies{{
           key
           body_code
           name
          }}
        }}
        meta.body_pieces{{
          key
          outline
          resonance_color_inches
          type
         
        }}
      }}
    }}"""

    df = provider.query_dataframe(q, expand_levels=expand_levels)
    df = res.utils.dataframes.rename_and_whitelist(df, columns=column_mapping)

    # df['key'] = map the one number from a key lookup to the spec

    return df


####
## sample data use body and key for cc-6001 or another example like tk-6093


def query_one_specification_by_one_numbers(
    one_numbers, body, version, res_sell_size, **kwargs
):
    """
    TODO: store a relationship between the style/one -> one_spec - for now generating some test data

    For now returning test data until we complete the make body onboarding flow
    Asset structure is :
    {'piece_type': 'self',
      'asset_key': 'CC-6001-V8-BELT_SM_000',
      'geometry': 'LINEARRING (....)',
      'resonance_color_inches': {'length': '2.0',
        'angle': '-90.0',
        'piece_bounds': '(4.2264, -2.1352, 44.4452, 2.6148)',
        'label_corners': 'MULTIPOINT (4.2314 2.2398, 4.2314 1.393133333333333, 4.364733333333334 2.2398, 4.364733333333334 1.393133333333333)'},
      'key': '10101010',
      'color_code': 'PERPIT',
      'material_code': 'CTW70',
      'file': 's3://res-data-platform/samples/images/body_pieces/cc-6001/CC-6001-V8-BELT_SM_000.png'}

    """
    if not isinstance(one_numbers, list):
        one_numbers = [one_numbers]

    l = ["body_piece_key", "color_code", "material_code", "one_specification_key"]
    d = dict(zip(l, l))
    d["meta.body_pieces_type"] = "piece_type"
    d["meta.body_pieces_outline"] = "geometry"
    d["meta.body_pieces_resonance_color_inches"] = "resonance_color_inches"

    kwargs["column_mapping"] = d

    df = query_one_specification_by_key(
        body=body, version=version, res_sell_size=res_sell_size, **kwargs
    )

    # should move file resolution inside the nest interface TODO move CC_6001 to the right location
    # df["file"] = df["body_piece_key"].map(
    #     lambda x: f"s3://res-data-platform/samples/images/body_pieces/{body.lower()}/{x}.png"
    # )

    # the piece spec should now this and the size
    color = "perpit"

    # NOTE: applied two conventions (lower case and underscores) to s3 paths - we should find a place to centralize this
    suffix = f"{body}/{version}/{color}/{res_sell_size}/named_pieces".lower().replace(
        "-", "_"
    )
    path = f"s3://meta-one-assets-prod/color_on_shape/{suffix}"
    df["file"] = df["body_piece_key"].map(lambda x: f"{path}/{x}.png")

    # more temporary coercing as we figure out the contract
    df["resonance_color_inches"] = df["resonance_color_inches"].map(
        lambda s: literal_eval(s) if isinstance(s, str) else s
    )

    # for now i want to do this for serialization purposes
    # this makes transport easier and we can always interpret when it gets sent over the wire at destination
    # technically it would be better to always hold onto the shapely object if we can trust clients to know how to marshall on post etc.
    df["geometry"] = df["geometry"].map(str)

    # test for now
    df["key"] = one_numbers[0]

    return df


def get_one_spec_pieces(
    key,
    filter_piece_types=None,
    filter_piece_names=None,
    filter_piece_materials=None,
    unmarshall_geometry=False,
):
    """
    example key = "CC-6001-V10-2ZZSM-PERPIT-B940A8ED1A"

    """

    def loads_geom(g):
        from shapely.wkt import loads

        return loads(g) if not pd.isnull(g) else None

    def color_root_path_from_key(k, root="s3://meta-one-assets-prod/color_on_shape"):
        """
        example: CC-6001-V10-2ZZSM-PERPIT-B940A8ED1A
                 <0:brand>-<1:body_number>-<2:version>-<3:size>-<4:color>-<5:hash>
        """
        parts = k.split("-")
        body = "-".join(parts[:2])
        version = parts[2]
        res_sell_size = parts[3]
        color = parts[4]
        suffix = (
            f"{body}/{version}/{color}/{res_sell_size}/named_pieces".lower().replace(
                "-", "_"
            )
        )

        return f"{root}/{suffix}"

    provider = res.connectors.load("dgraph")

    q = f"""{{
      get(func: type(make.body_pieces))
      @filter(eq(one_specification_key, {key}) )
      {{
        uid
        key
        body_piece_key
        color_code
        material_code
        one_specification_key
        make.one_specification{{
          key
          body_key
          meta.bodies{{
           key
           body_code
           name
          }}
        }}
        meta.body_pieces{{
          key
          geometry
          notches
          internal_lines
          viable_surface
          type
        }}
      }}
    }}"""

    df = provider.query_dataframe(
        q,
        expand_levels={"make.one_specification": 1, "meta.body_pieces": 1},
    )

    cols = [
        "one_specification_key",
        "body_piece_key",
        "meta.body_pieces_geometry",
        "meta.body_pieces_type",
        "meta.body_pieces_notches",
        "meta.body_pieces_internal_lines",
        "meta.body_pieces_viable_surface",
        "material_code",
    ]

    map = {
        "body_piece_key": "body_piece_key",
        "meta.body_pieces_geometry": "geometry",
        "meta.body_pieces_notches": "notches",
        "meta.body_pieces_internal_lines": "internal_lines",
        "meta.body_pieces_type": "piece_type",
        "meta.body_pieces_viable_surface": "viable_surface",
    }

    cols = [c for c in cols if c in df.columns]

    map = {k: v for k, v in map.items() if k in cols}

    df = df[cols].rename(columns=map)

    if unmarshall_geometry:
        # TODO: contract
        for g in ["geometry", "notches", "internal_lines", "viable_surface"]:
            if g in df.columns:
                df[g] = df[g].map(loads_geom)

    df["file"] = df.apply(
        lambda row: f"{color_root_path_from_key(row['one_specification_key'])}/{row['body_piece_key']}.png",
        axis=1,
    )
    return df
