"""
experimental: wrapper around s3 file listings 
should evolve into some conventions for datalake
"""

import res
import pandas as pd


def get_net_file(body, size, version=None, combo=False):
    """
    's3://meta-one-assets-prod/bodies/net_files/jr_3055/v4/jr_3055_v4_net_2x_15x15.dxf',
    's3://meta-one-assets-prod/bodies/net_files/jr_3055/v4/jr_3055_v4_net_2x_c_15x15.dxf',

    's3://meta-one-assets-prod/bodies/net_files/bg_6001/v3/bg_6001_v3_net_2x_10x10.dxf'

    """

    s3 = res.connectors.load("s3")
    body = body.lower().replace("-", "_")
    size = size.lower()
    data = pd.DataFrame(
        s3.ls_info(f"s3://meta-one-assets-prod/bodies/net_files/{body}")
    )
    data["version"] = data["path"].map(lambda p: p.split("/")[6])
    # this is a bit cheeky - think more about it

    data = data[data["path"].map(lambda pth: f"net_{size}_" in pth)]

    def find_size(p):
        i = p.index(f"net_{size}")
        return p[i + 4 : i + 4 + len(size)].upper()

    data["size"] = data["path"].map(find_size)

    data["is_combo"] = data["path"].map(lambda p: f"net_{size}_c" in p)

    if version:
        if "v" not in version.lower():
            version = f"v{version}"
        data = data[data["version"] == version]
    data = data.sort_values("last_modified", ascending=False)

    data = data[data["is_combo"] == combo]

    return data


def dxf_metadata(filepath):
    s3 = res.connectors.load("s3")
    try:
        d = s3.read(filepath)
        piece_names = list(d.get_sized_geometries(size=d.sample_size)["res.piece_name"])

        def part(p):
            ps = p.split("-")
            for i, v in enumerate(ps):
                if v[0] == "V":
                    break
            return "-".join(ps[i + 1 :])

        piece_names = [part(p) for p in piece_names]

        return {
            "sizes": d.sizes,
            "piece_codes": d.primary_piece_codes,
            "res_piece_names": piece_names,
        }
    except Exception as ex:
        return {"errors": repr(ex)}


def list_astms(body, min_version=None, modified_after=None, drop_dups=True, flow="2d"):

    if flow == "2d":
        return list_astms_2d(body, min_version, modified_after, drop_dups)
    if flow == "3d":
        return list_astms_3d(body, min_version, modified_after, drop_dups)
    raise Exception(f"unsupported flow type {flow}")


def list_astms_3d(body, min_version=None, modified_after=None, drop_dups=True):
    """
    We list astms from 2d oe 3d data lake sources
    - 2d
    - 3d

        path = "s3://meta-one-assets-prod/bodies/3d_body_files/[body_code]/v[body_version]/extracted/*""

    assumptions there is only one dxf under the extracted path

    list_astms_3d("TT-3036") or None param to test all bodies

    """
    s3 = res.connectors.load("s3")

    def body_from_path(s):
        idx = s.index("3d_body_files")
        body = s[idx:].split("/")[1].lower()  # .replace('-','_')
        return body

    def _version(s):
        try:
            # filename
            s = s.split("/")[6]
            # version part of the file name
            version = int(s.replace("v", ""))
            return version
        except:
            res.utils.logger.debug(f"Failed to extract version from this path {s}")
            return -1

    if body:
        b = body.lower().replace("-", "_")
        l = list(
            s3.ls_info(
                f"s3://meta-one-assets-prod/bodies/3d_body_files/{b}",
                modified_after=modified_after,
            )
        )
    else:
        l = list(
            s3.ls_info(
                f"s3://meta-one-assets-prod/bodies/3d_body_files/",
                modified_after=modified_after,
            )
        )

    l = [item for item in l if ".dxf" in item["path"] and "extracted" in item["path"]]

    df = pd.DataFrame(l)
    if len(df):
        df["body_code"] = df["path"].map(body_from_path)
        df["meta_version"] = df["path"].map(_version).astype(int)
        if min_version:
            df = df[df["meta_version"] >= min_version].reset_index().drop("index", 1)

        df["body_code"] = df["body_code"].map(lambda b: b.upper().replace("_", "-"))
        df["lowered_body_code"] = df["body_code"].map(
            lambda x: x.replace("-", "_").lower()
        )

        # drop the file size
        df = df.drop("size", 1)

    if drop_dups and len(df):
        return (
            df.sort_values(["meta_version", "last_modified"])
            .drop_duplicates(subset=["body_code"], keep="last")
            .reset_index()
            .drop("index", 1)
        )
    return df


def list_astms_2d(body, min_version=None, modified_after=None, drop_dups=True):
    """
    We list astms from 2d oe 3d data lake sources
    - 2d
    - 3d

    """
    s3 = res.connectors.load("s3")

    def body_from_path(s):
        idx = s.index("bodies")
        body = s[idx:].split("/")[1].lower()  # .replace('-','_')
        return body

    def _version(s):
        try:
            # filename
            b = body_from_path(s)
            s = s.split("/")[-1]

            # version part of the file name
            version = int(s[s.index(b) :].split("_")[2].replace("v", ""))
            return version
        except:
            # res.utils.logger.warn(f"Failed to extract version from this path {s}")
            return -1

    if body:
        b = body.lower().replace("-", "_")
        l = list(
            s3.ls_info(
                f"s3://meta-one-assets-prod/bodies/{b}/pattern_files",
                modified_after=modified_after,
            )
        )
    else:
        l = list(
            s3.ls_info(
                f"s3://meta-one-assets-prod/bodies/", modified_after=modified_after
            )
        )
        l = [item for item in l if ".dxf" in item["path"]]

    df = pd.DataFrame(l)
    if len(df):
        df["body_code"] = df["path"].map(body_from_path)
        df["meta_version"] = df["path"].map(_version).astype(int)
        if min_version:
            df = df[df["meta_version"] >= min_version].reset_index().drop("index", 1)

        df["body_code"] = df["body_code"].map(lambda b: b.upper().replace("_", "-"))
        df["lowered_body_code"] = df["body_code"].map(
            lambda x: x.replace("-", "_").lower()
        )

        # drop the file size
        df = df.drop("size", 1)

    if drop_dups and len(df):
        return (
            df.sort_values(["meta_version", "last_modified"])
            .drop_duplicates(subset=["body_code"], keep="last")
            .reset_index()
            .drop("index", 1)
        )
    return df


def list_body_colors(
    body,
    min_version=None,
    modified_after=None,
    add_normed_size=True,
    drop_dups=True,
    paths_only=True,
):
    s3 = res.connectors.load("s3")

    def body_from_path(s):
        idx = s.index("color_on_shape")
        body = s[idx:].split("/")[1].lower()  # .replace('-','_')
        return body

    def _version(s):
        s = s["path"] if isinstance(s, dict) else s
        b = body_from_path(s)
        version = int(s[s.index(b) :].split("/")[1].replace("v", ""))
        return version

    def _color(s):
        s = s["path"] if isinstance(s, dict) else s
        b = body_from_path(s)
        version = s[s.index(b) :].split("/")[2].upper()
        return version

    def _size(s):
        s = s["path"] if isinstance(s, dict) else s
        b = body_from_path(s)
        version = s[s.index(b) :].split("/")[3].upper()
        return version

    if body == None:
        l = list(
            s3.ls_info(
                f"s3://meta-one-assets-prod/color_on_shape/",
                modified_after=modified_after,
            )
        )
    else:
        b = body.lower().replace("-", "_")
        l = list(
            s3.ls_info(
                f"s3://meta-one-assets-prod/color_on_shape/{b}",
                modified_after=modified_after,
            )
        )

    # currently these are the conditions we will accept: some sort of archive
    # we could allow inzipped pieces loose too but dont want to confuse things
    l = [
        item
        for item in l
        if "rasterized_files" in item["path"] or "zipped_pieces" in item["path"]
    ]

    if min_version:
        l = [item for item in l if _version(item) >= min_version]

    df = pd.DataFrame(l)
    df["body_code"] = df["path"].map(body_from_path)
    df = df[df["body_code"].map(lambda x: "-" not in x)].reset_index().drop("index", 1)
    df["color_code"] = df["path"].map(_color)
    df["color_meta_version"] = df["path"].map(_version)
    df["size"] = df["path"].map(_size)

    df["body_code"] = df["body_code"].map(lambda b: b.upper().replace("_", "-"))
    df["flow_ref"] = "GERBER_TO_RASTER"

    if add_normed_size:
        res.utils.logger.info("adding size mapping from airtable or cache....")
        airtable = res.connectors.load("airtable")
        sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
        # try the cache - this is temporary as we migrate from airtable but want to test batch jobs
        slu = s3.read("s3://res-data-development/cache/sizes.csv")
        # TODO - check if we are missing a mapping or refresh??
        # slu = sizes_lu.to_dataframe(fields=["_accountingsku", "Size Normalized"])
        slu = dict(slu[["_accountingsku", "Size Normalized"]].values)

        df["normed_size"] = df["size"].map(lambda s: slu.get(s))

    if paths_only:
        # archives are paths in this context
        df["path"] = df["path"].map(
            lambda f: s3.object_dir(f) if ".zip" not in f else f
        )
        # this is temp as we determine the flows
        df.loc[df["path"].map(lambda s: ".zip" in s), "flow_ref"] = "BZW_TO_ZIP"

    if drop_dups:
        return (
            df.sort_values(["color_meta_version", "last_modified"])
            .drop_duplicates(subset=["body_code", "color_code", "size"], keep="last")
            .reset_index()
            .drop("index", 1)
        )

    return df
