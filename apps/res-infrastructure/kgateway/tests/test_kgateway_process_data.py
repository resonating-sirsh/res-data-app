from ..src import main


class TestProcessData:
    def test_json_not_dict(self):
        request_dict = {"data": '{"test":123}'}
        err, processed_data = main.processData(request_dict)
        assert err is not None
        assert err == "Error: data must be an object, not a JSON string"
        assert processed_data is None

    def test_good_json(self):
        request_dict = {"data": {"test": 123}}
        err, processed_data = main.processData(request_dict)
        assert err is None
        assert processed_data == request_dict["data"]

    def test_bad_xml(self):
        request_dict = {"data": "<aABC</a>", "data_format": "xml"}
        err, processed_data = main.processData(request_dict)
        assert err is not None
        assert "Error parsing XML" in err
        assert processed_data is None

    def test_good_xml(self):
        request_dict = {"data": "<a>ABC</a>", "data_format": "xml"}
        err, processed_data = main.processData(request_dict)
        assert err is None
        assert processed_data == {"a": "ABC"}
