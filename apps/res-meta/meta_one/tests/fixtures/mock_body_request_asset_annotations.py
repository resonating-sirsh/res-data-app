from datetime import datetime
from typing import List, Optional
from uuid import UUID
from source.routes.body_request_annotations.controllers import (
    BodyRequestAnnotationController,
    BodyRequestAnnotation,
    BodyRequestAssetAnnotationType,
)


class MockBodyRequestAssetAnnotationController(BodyRequestAnnotationController):
    nodes: List[BodyRequestAnnotation] = []

    def __init__(self):
        self.nodes = []

    def find_by_id(self, id: UUID) -> Optional[BodyRequestAnnotation]:
        for node in self.nodes:
            if node.id == str(id):
                return node
        return None

    def find(
        self,
        ids: Optional[List[UUID]] = [],
        asset_id: Optional[UUID] = None,
        annotation_type: Optional[BodyRequestAssetAnnotationType] = None,
        limit: int = 100,
    ) -> List[BodyRequestAnnotation]:
        nodes = []
        for node in self.nodes:
            if asset_id and node.asset_id != asset_id:
                continue
            if ids and UUID(node.id) not in ids:
                continue
            if annotation_type and node.annotation_type != annotation_type:
                continue
            nodes.append(node)
        return nodes[0:limit]

    def save(self, record: BodyRequestAnnotation) -> BodyRequestAnnotation:
        for node in self.nodes:
            if node.id == record.id:
                node = record
                return node
        self.nodes.append(record)
        return record

    def delete(self, ids: List[UUID] = [], hard=True) -> bool:
        if hard:
            self.nodes = [node for node in self.nodes if UUID(node.id) not in ids]
        else:
            self.nodes = [
                BodyRequestAnnotation(
                    deleted_at=datetime.now(),
                    **node.dict(exclude={"deleted_at"}),
                )
                for node in self.nodes
                if UUID(node.id) in ids
            ]
        return True
