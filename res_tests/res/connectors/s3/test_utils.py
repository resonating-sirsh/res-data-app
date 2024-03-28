from res.connectors.s3.utils import is_s3_uri


class TestS3Utils:
    def test_is_s3_uri(self):
        assert is_s3_uri("s3://test/test.jpg") == True

    def test_is_not_s3_uri(self):
        assert is_s3_uri("test/test.jpg") == False
