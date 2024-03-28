from uuid import uuid4
from source.routes.label_styles.controller import LabelStyleController, LabelStyle, List


class MockLabelStyleController(LabelStyleController):
    def find(self, limit: int = 100) -> List[LabelStyle]:
        return [
            LabelStyle(
                id=str(uuid4()),
                name="Test Label Style",
                thumbnail_url="s3://test-bucket/test-label-style-thumbnail.png",
                dimensions="100x100",
                body_code="test-body-code",
            )
        ]
