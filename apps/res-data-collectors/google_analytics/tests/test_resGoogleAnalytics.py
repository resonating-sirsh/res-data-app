from ..src.ResGAFakeService import FakeAnalyticsServiceObject
from ..src.ResGoogleAnalytics import ResGoogleAnalyticsClient


class TestConvertData:
    def test_convert_orders(self):
        gac = ResGoogleAnalyticsClient(FakeAnalyticsServiceObject())
        data = gac.run_query_for_day("123", type="orders")
        converted_data = gac.convert_query_results(data, "TT")
        expected_data = [
            {
                "ga_campaign": "a",
                "ga_date": "b",
                "ga_transactionId": "c",
                "ga_channelGrouping": "d",
                "ga_source": "e",
                "ga_medium": "f",
                "ga_adDistributionNetwork": "g",
                "ga_transactions": 6.6,
                "ga_transactionRevenue": 2.0,
                "ga_sessions": 3.1,
                "ga_users": 4.0,
                "ga_newUsers": 5.0,
                "ga_bounces": 7.789,
                "brand_code": "TT",
                "primary_key": "a-b-c-d-e-f-g-TT",
            }
        ]
        assert converted_data == expected_data
