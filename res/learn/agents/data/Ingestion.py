import res
from schemas.pydantic.common import FlowApiAgentMemoryModel
from typing import List
import pandas as pd
from res.utils.env import RES_DATA_BUCKET
from res.learn.agents.data.EntityDataStore import EntityDataStore
from res.learn.agents.data.VectorDataStore import VectorDataStore
from res.learn.agents.data.ColumnarDataStore import ColumnarDataStore

from tqdm import tqdm


class IngestionManager:
    """
    Ingestion entites leaning on a pydantic schema

    For example
        ```
        class TestAgentMemory(FlowApiAgentMemoryModel):
            class Config(CommonConfig):
                entity_name = "test"
                namespace = "test"
            id: str = Field(alias = 'key_field')
            #default can be something
            stat_count: Optional[int] = Field(default=0)
            #booleans should default to
            is_true: int = Field(default=0)
            a_list: List[str] = Field(default_factory=list)
            text: Optional[str] = Field(is_large_text=True)

        tt = TestAgentMemory(**{
            "key_field": '123',
        })
        ```

        The idea is to some simple config on the object to decide what makes sense
        to send to any given store and then we can just feed data from any system

        The data owner configures their keys, entity name/namespace and what fields should go to what stores using field attributes
        is_large_text: will make it go to the vector store only - otherwise only string types go to the vector store for now
        everything except large text will go to the other stores for now - for example we dont want large text in columnar or redis tables for different reasons

    """

    def __init__(self):
        self._s3 = res.connectors.load("s3")

    def ingest_entities(self, data: List[FlowApiAgentMemoryModel]):
        if data:
            delegate = data[0]

            namespace = delegate.Config.namespace
            entity_name = delegate.Config.entity_name

            res.utils.logger.info("Ingesting stats tool columnar data")
            ColumnarDataStore(namespace, entity_name).add(data)
            res.utils.logger.info("Ingesting entity resolution attribute data")
            EntityDataStore(namespace, entity_name).add(data)
            res.utils.logger.info("Ingesting further details tools text/vector data")
            VectorDataStore(namespace, entity_name).add(data)

        res.utils.logger.info("Done")

        return {}
