import os
import requests
import pandas as pd
from stringcase import snakecase
import res


class CodaConnector:
    def __init__(self):
        self._header = {"Authorization": f"Bearer {os.environ['CODA_API_KEY']}"}

    def _get_items(self, uri, retries=0, **kwargs):
        res.utils.logger.info(uri)
        response = requests.get(uri, headers=self._header, params=kwargs)

        if response.status_code == 200:
            response = response.json()

            if response.get("items"):
                yield response["items"],
            if response.get("nextPageLink"):
                for r in self._get_items(response.get("nextPageLink")):
                    yield r
        elif retries < 3:
            print(response.text, "retry")
            for r in self._get_items(uri, retries=retries + 1):
                yield r

    def get_docs(self, **kwargs):
        uri = "https://coda.io/apis/v1/docs"

        data = []

        for batch in self._get_items(uri):
            data += batch
            yield batch

        # data = pd.DataFrame(data)[
        #     [
        #         "id",
        #         "type",
        #         "name",
        #         "href",
        #         "browserLink",
        #         "folder",
        #         "createdAt",
        #         "updatedAt",
        #     ]
        # ]

        # data.columns = [snakecase(c) for c in data.columns]

        return data

    def get_doc_pages(self, doc_id, **kwargs):
        uri = f"https://coda.io/apis/v1/docs/{doc_id}/pages"

        data = []

        for batch in self._get_items(uri):
            # data += batch
            yield batch

        # cols = [
        #     "id",
        #     "type",
        #     "name",
        #     "href",
        #     "browserLink",
        #     "folder",
        #     "createdAt",
        #     "updatedAt",
        #     "children",
        #     "parent",
        #     "contentType",
        # ]
        # data = pd.DataFrame(data)
        # data = data[[c for c in cols if c in data.columns]]
        # data.columns = [snakecase(c) for c in data.columns]

        return data

    def get_page_detail(self, uri, **kwargs):
        return requests.get(uri, headers=self._header).json()

    def get_page_content(self, uri, **kwargs):
        """
        downloads the markdown for the page
        """
        response = requests.post(
            f"{uri}/export", json={"outputFormat": "markdown"}, headers=self._header
        )

        if response.status_code != 200:
            data = response.json()
            while data.get("status") == "inProgress":
                response = requests.get(data["href"], headers=self._header)
                data = response.json()

        return requests.get(data["downloadLink"]).text


def load_coda_doc(name, did, since_date=None):
    """

    for example: load_coda_doc('3dm1', 'HmExcWtlGz')
    """
    from res.observability.entity import AbstractVectorStoreEntry
    from res.observability.io import VectorDataStore
    from res.connectors import load

    coda = load("coda")
    Model = AbstractVectorStoreEntry.create_model(name, namespace="coda")
    store = VectorDataStore(Model)

    def callback(items):
        """
        batch processing
        """
        res.utils.logger.info(f"In the callback processing {len(items)} items")
        records = []
        for batch in items:
            for record in batch:
                res.utils.logger.info(record["href"])
                try:
                    page_data = coda.get_page_content(record["href"])
                except:
                    continue
                record = Model(
                    name=record["name"], text=page_data, doc_id=record["name"]
                )
                records.append(record)
        store.add(records)

    for batch in coda.get_doc_pages(did):
        callback(batch)


def ingest_coda_docs(date=None, filter_keys=None):
    if date is None:
        date = res.utils.dates.relative_to_now(-7)

    # hard code what we index for now - maps: name <- multiple doc ids
    config = {
        "make_svot": ["vOdUsYzMGj", "uABQ6Nj6Ys", "nPeMu5GSsW"],
        "3dm1": "HmExcWtlGz",
        # "technology": "ZNX5Sf3x2R",
        "product-planning": "TAAw5ucw7x",
        "sew-development": "A6wjBOi1Cp",
        "resonance-contracts": "vjyCY1qJSp",
        # "resonance-genome": "zktiW12jTY",
        "make_svot": "uABQ6Nj6Ys",
    }

    from res.observability.io import list_stores
    import pandas as pd
    import traceback

    # df = pd.DataFrame(list_stores())
    # df = df[df["namespace"] == "coda"]
    # coda_docs = df["name"].unique()

    for name, ids in config.items():
        if filter_keys and name not in filter_keys:
            continue
        if not isinstance(ids, list):
            ids = [ids]
        for id in ids:
            res.utils.logger.info(f"{name, id}")

            try:
                load_coda_doc(name=name, did=id, since_date=date)
            except:
                res.utils.logger.warn(
                    f"Error ingesting doc {name}:> {traceback.format_exc()}"
                )
