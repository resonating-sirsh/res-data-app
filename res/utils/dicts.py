from typing import Callable, Any, List, Dict, Union


def index_dicts(dicts: List[Dict], key_generator: Callable) -> Dict[Any, Dict]:
    """
    Indexes a list of dictionaries by a key generator function.

    Example:
        example = [
            { 'name': 'Double Overlap Button Placket', 'code': 'DBP' },
            { 'name': 'Jersey Heavy', 'code': 'JHY' },
        ]

        index_dicts(example, lambda d: d['code']) == {
            'DBP': { 'name': 'Double Overlap Button Placket', 'code': 'DBP' },
            'JHY': { 'name': 'Jersey Heavy', 'code': 'JHY' },
        }
    """
    result = {}
    for d in dicts:
        result[key_generator(d)] = d
    return result


def traverse_dict(d: Dict, path: Union[Dict, str, List, tuple]) -> Any:
    """
    Extracts values from a nested dictionary using a traversal path.

    Example:
        example = {'name': 'Double Overlap Button Placket',
            'code': 'DBP',
            'use': None,
            'materials': None,
            'metadata': {'record_id': 'recAtl8ed7LfCKrqe',
            'sewing_cells': None,
            'airtable_created_at': '2020-12-01T19:42:34.000Z',
            'airtable_updated_at': '2023-09-06T13:09:23.000Z'}}

        traverse_dict(example, 'code') == 'DBP'
        traverse_dict(example, { 'metadata': 'record_id' }) == 'recAtl8ed7LfCKrqe'
        traverse_dict(example, ['metadata', 'record_id']) == 'recAtl8ed7LfCKrqe'
    """
    if isinstance(path, str):
        return d[path]
    if isinstance(path, dict):
        key = list(path.keys())[0]
        value = list(path.values())[0]
        return traverse_dict(d[key], value)
    if len(path) == 2:
        return traverse_dict(d[path[0]], path[1])
    return traverse_dict(d[path[0]], path[1:])


def group_by(ds: dict, key: Any, keep_key: bool = True) -> Dict[Any, List[Dict]]:
    """
    Groups a list of dictionaries by a key.

    Example:
        example = [
            { 'name': 'Double Overlap Button Placket', 'code': 'DBP' },
            { 'name': 'Jersey Heavy', 'code': 'JHY' },
        ]

        group_by(example, 'code') == {
            'DBP': [{ 'name': 'Double Overlap Button Placket', 'code': 'DBP' }],
            'JHY': [{ 'name': 'Jersey Heavy', 'code': 'JHY' }],
        }
    """
    result = {}
    for d in ds:
        k = d[key]
        if k not in result:
            result[k] = []
        if not keep_key:
            d = {k: v for k, v in d.items() if k != key}
        result[k].append(d)
    return result
