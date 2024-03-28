import pytest
from source.routes.label_styles.controller import LabelStyleAirtableConnector


@pytest.mark.skip("Skipping test_label")
class TestLabelStyleController:
    def test_get_all_label_style(self):
        controller = LabelStyleAirtableConnector()

        label_styles = controller.find()

        assert type(label_styles) == list
        assert len(label_styles) > 0
        assert type(label_styles[0].id) == str
        assert type(label_styles[0].name) == str
        assert type(label_styles[0].thumbnail_url) == str
        assert type(label_styles[0].dimensions) == str
        assert type(label_styles[0].body_code) == str
