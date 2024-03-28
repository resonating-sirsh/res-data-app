def is_s3_uri(uri: str) -> bool:
    """Validate that the s3 path is valid."""
    return uri.startswith("s3://")
