from ..src.main import get_response_status


class TestGetResponseStatus:
    def test_get_response_status(self):
        response_str_ok = '{"page":{"id":"5vv477bkm0kl","name":"Airtable","url":"https://status.airtable.com","time_zone":"Etc/UTC","updated_at":"2021-06-25T21:23:57.205Z"},"status":{"indicator":"none","description":"All Systems Operational"}}'
        response_str_bad = '{"page":{"id":"94s7z8vpy1n8","name":"Snowflake","url":"https://status.snowflake.com","time_zone":"America/Los_Angeles","updated_at":"2021-06-30T11:28:03.815-07:00"},"status":{"indicator":"critical","description":"Major System Outage"}}'
        assert get_response_status(response_str_ok) == "none"
        assert get_response_status(response_str_bad) == "critical"
