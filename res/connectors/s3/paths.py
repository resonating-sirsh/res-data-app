from res.utils import logger


def dxf_file_path(*, body_code: str, version: str, env="prod"):
    if "-" in body_code:
        logger.warn("Replacing '-' with '_' in body_code", received=body_code)
        body_code = body_code.replace("-", "_")
    return f"s3://meta-one-assets-{env}/bodies/{body_code}/pattern_files/body_{body_code}_{version}_pattern.dxf"


def marker_image_dir(
    *, body_code: str, version: str, color: str, size: str, env="prod"
):
    if "_" in body_code:
        logger.warn("Replacing '_' with '-' in body_code", received=body_code)
        body_code = body_code.replace("_", "-")
    return f"s3://meta-one-assets-{env}/color_on_shape/{body_code}/{version}/{color}/{size}/pieces/"


# Generic Lookup for paths above
_types_to_lookups = {"DXF_FILE": dxf_file_path, "MARKER_IMAGE_DIR": marker_image_dir}


def get_asset_path(asset_type, **kwargs):
    # e.g. get_asset_path('DXF_FILE', ...kwargs)
    lookup_fn = _types_to_lookups.get(asset_type, None)
    if lookup_fn is None:
        raise Exception(
            f"Invalid asset type received: {asset_type}. Should be one of {list(_types_to_lookups.keys())}"
        )
    return lookup_fn(**kwargs)
