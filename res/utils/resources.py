from pathlib import Path


def get_resource_path(key, ext=None):
    return Path(__file__).parent.parent.parent / "resources" / key


def read(key, **kwargs):
    from res.media.images.icons import get_svg

    path = get_resource_path(key)
    if not path.exists():
        raise Exception(f"Cannot load the resource {path}")

    if path.suffix == ".svg":
        return get_svg(path.stem, root=str(path.parent), **kwargs)
    with open(key, "rb") as f:
        return f.read()
