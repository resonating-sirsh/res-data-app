def to_obj(error, message, detail):
    return {
        "error": error,
        "message": message,
        "detail": detail,
    }


# basically an enum so you don't have to "" all the time
class ErrorCodes:
    UNKNOWN_ERROR = to_obj("UNKNOWN_ERROR", "Unknown error", "Unknown error")
    IMAGE_DPI_NOT_SUPPORTED = to_obj(
        "IMAGE_DPI_NOT_SUPPORTED", "Image DPI not supported", "Ensure image is 300 DPI"
    )
    COLOR_TYPE_NOT_SUPPORTED = to_obj(
        "COLOR_TYPE_NOT_SUPPORTED",
        "Color type not supported",
        "Ensure color type is RGB and not CMYK etc.",
    )
    IMAGE_NOT_FOUND = to_obj(
        "IMAGE_NOT_FOUND",
        "Image not found",
        "Ensure image exists on S3 and uri is valid",
    )

    @staticmethod
    def remove_duplicates(errors):
        result = {e["error"]: e for e in errors}
        return list(result.values())
