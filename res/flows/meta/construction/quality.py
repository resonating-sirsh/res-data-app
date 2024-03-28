import res
from stringcase import snakecase
import requests
from PIL import Image
from io import BytesIO
import json
from res.observability.io import VectorDataStore, ColumnarDataStore
from res.observability.entity import AbstractVectorStoreEntry, AbstractEntity


def dump_quality():
    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")
    data = airtable.get_table_data("app6tdm0kPbc24p1c/tblrcdV9eWokVENno")

    def get_image_field(field):
        def get_image(row):
            root = f"s3://res-data-platform/airtable/images/app6tdm0kPbc24p1c/tblrcdV9eWokVENno/{field}/{row['record_id']}"
            try:
                files = [i["url"] for i in row[field]]
                out_files = []
                for i, f in enumerate(files):
                    file = f"{root}/{i}.png"
                    response = requests.get(f)
                    image = Image.open(BytesIO(response.content))
                    out_files.append(file)
                    s3.write(file, image)

                return out_files
            except Exception as ex:
                # print(ex)
                return

        return get_image

    data = data.rename(columns={"__timestamp__": "timestamp"})
    data.columns = [snakecase(c.replace(" ", "")) for c in data.columns]
    d = res.utils.dataframes.replace_nan_with_none(data)
    d["created_by"] = d["created_by"].map(lambda x: x["email"])
    d["ideal_result"] = d.apply(get_image_field("ideal_result"), axis=1)
    d["error_images"] = d.apply(get_image_field("error_images"), axis=1)
    d["text"] = d.apply(lambda row: json.dumps(row, default=str), axis=1)
    d = d[
        [
            "name",
            "quality_category",
            "rule",
            "quality_check",
            "ideal_result",
            "error_images",
            "created_by",
        ]
    ]
    d["text"] = d.apply(lambda row: dict(row), axis=1)
    d["text"] = d["text"].map(json.dumps)

    # purge
    # for f in s3.ls('s3://res-data-platform/stores/vector-store/quality'):
    # s3._delete_object(f)
    Model = AbstractVectorStoreEntry.create_model(
        namespace="quality", name="quality_assessment"
    )
    records = [Model(**item) for item in d.to_dict("records")]
    store = VectorDataStore(
        Model,
        description="guidelines on quality checks for garments after sew",
        create_if_not_found=True,
    )
    store.add(records)

    # Model = AbstractEntity.create_model_from_data(namespace='test', name='qa_assessment', data=d)

    return d
