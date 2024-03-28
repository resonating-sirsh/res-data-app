import typing

from pydantic import BaseModel


class ProductPhotos(BaseModel):
    id: str
    key: str
    bucket: str
    name: str
    size: int
    type: str

    def __init__(self, data: typing.Dict[str, typing.Any]):
        super().__init__(
            id=data["id"],
            key=data["key"],
            size=data["size"],
            name=data["name"],
            bucket=data["bucket"],
            type=data["type"]
            if data["type"] not in "application/octet-stream"
            else "model/gltf-binary",
        )


class Product(BaseModel):
    id: str
    name: str
    storeCode: str
    ecommerceId: str
    productHandle: str
    photos: typing.List[ProductPhotos]

    def __init__(self, data: typing.Dict[str, typing.Any]):
        photos = [ProductPhotos(photo) for photo in data["photos"]]
        super().__init__(
            id=data["id"],
            name=data["name"],
            storeCode=data["storeCode"],
            ecommerceId=data["ecommerceId"],
            productHandle=data["productHandle"],
            photos=photos,
        )

    def dict(self, *args, **kwargs) -> typing.Dict[str, typing.Any]:
        response = super().dict(*args, **kwargs)
        response["photos"] = [photo.dict() for photo in response["photos"]]
        return response
