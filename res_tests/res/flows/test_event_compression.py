from res.flows.event_compression import compress_event, decompress_event


def test_event_compression():
    event = {
        "foo": "bar",
        "baz": ["asd", "asd", "sdfsdf"],
        "assets": [{"a": 1, "b": 2}, {"a": 2}],
    }
    assert event == decompress_event(compress_event(event))
    assert isinstance(compress_event(event), str)
