from ..src import main
import json


class TestValidation:
    def test_bad_json(self):
        err, parsed_data = main.validateRequest("teststring")
        assert err is not None
        assert err == "Expecting value: line 1 column 1 (char 0)"
        assert parsed_data is None

    def test_good_json_bad_schema(self):
        err, parsed_data = main.validateRequest('{"test":123}')
        assert err is not None
        assert "Failed validating" in err
        assert parsed_data is None

    def test_good_data_json(self):
        data = """{
            "version":"1.0.0",
            "process_name":"pytest",
            "topic":"testing_validation",
            "data":{"a":123}
        }"""
        err, parsed_data = main.validateRequest(data)
        assert err is None
        assert parsed_data == json.loads(data)

    def test_good_data_xml(self):
        data = """{
            "version":"1.0.0",
            "process_name":"pytest",
            "topic":"testing_validation",
            "data":"<a>123</a>",
            "data_format":"xml"
        }"""
        err, parsed_data = main.validateRequest(data)
        assert err is None
        assert parsed_data == json.loads(data)
