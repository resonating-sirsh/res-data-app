import unittest
import pytest
from datetime import datetime, timedelta
from ..src.adwords_client import ResAdwordsClient


@pytest.mark.service
class TestAdwords(unittest.TestCase):
    def test_covering_ranges_same_start_end(self):
        d = datetime(2021, 6, 24)
        ranges = ResAdwordsClient._make_covering_date_ranges(d, d, 1)
        self.assertEqual([(d, d)], ranges)

    def test_covering_ranges_one_day(self):
        start = datetime(2021, 6, 24)
        end = start + timedelta(days=1)
        ranges = ResAdwordsClient._make_covering_date_ranges(start, end, 1)
        self.assertEqual([(start, end)], ranges)

    def test_covering_ranges(self):
        start = datetime(2021, 6, 1)
        end = datetime(2021, 6, 10)
        ranges = ResAdwordsClient._make_covering_date_ranges(start, end, 3)
        self.assertEqual(
            [
                (datetime(2021, 6, 1), datetime(2021, 6, 4)),
                (datetime(2021, 6, 5), datetime(2021, 6, 8)),
                (datetime(2021, 6, 9), datetime(2021, 6, 10)),
            ],
            ranges,
        )
