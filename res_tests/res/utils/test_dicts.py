from res.utils.dicts import group_by, traverse_dict, index_dicts


def test_index_dicts():
    example = [
        {"name": "Double Overlap Button Placket", "code": "DBP"},
        {"name": "Jersey Heavy", "code": "JHY"},
    ]

    assert index_dicts(example, lambda d: d["code"]) == {
        "DBP": {"name": "Double Overlap Button Placket", "code": "DBP"},
        "JHY": {"name": "Jersey Heavy", "code": "JHY"},
    }


def test_traverse_dict():
    example = {
        "name": "Double Overlap Button Placket",
        "code": "DBP",
        "use": None,
        "materials": None,
        "metadata": {
            "record_id": "recAtl8ed7LfCKrqe",
            "sewing_cells": None,
            "airtable_created_at": "2020-12-01T19:42:34.000Z",
            "airtable_updated_at": "2023-09-06T13:09:23.000Z",
        },
    }

    assert traverse_dict(example, "code") == "DBP"
    assert traverse_dict(example, {"metadata": "record_id"}) == "recAtl8ed7LfCKrqe"
    assert traverse_dict(example, ["metadata", "record_id"]) == "recAtl8ed7LfCKrqe"


def test_group_by():
    example = [
        {"name": "Double Overlap Button Placket", "code": "DBP"},
        {"name": "Jersey Heavy", "code": "JHY"},
    ]

    assert group_by(example, "code") == {
        "DBP": [{"name": "Double Overlap Button Placket", "code": "DBP"}],
        "JHY": [{"name": "Jersey Heavy", "code": "JHY"}],
    }
