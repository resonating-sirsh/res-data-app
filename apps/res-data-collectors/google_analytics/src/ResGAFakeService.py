class FakeAnalyticsServiceObject:
    def __init__(self):
        pass

    def reports(self):
        return self

    def batchGet(self, body):
        return self

    def execute(self):
        return {
            "reports": [
                {
                    "data": {
                        "rows": [
                            {
                                "dimensions": [
                                    "a",
                                    "b",
                                    "c",
                                    "d",
                                    "e",
                                    "f",
                                    "g",
                                ],
                                "metrics": [
                                    {
                                        "values": [
                                            "2.0",
                                            "3.1",
                                            "4",
                                            "5.0",
                                            "6.6",
                                            "7.789",
                                        ]
                                    }
                                ],
                            }
                        ],
                        "isDataGolden": True,
                    }
                }
            ],
        }
