import res


def test_load_callable():
    """
    pass either a module or a function on a module and make sure always resolves to a callable in flows
    """
    node = "flow_node_example"

    assert callable(
        res.flows.get_node(node)
    ), "get node did not return a callable function"

    node = "flow_node_example.handler"

    assert callable(
        res.flows.get_node(node)
    ), "get node did not return a callable function"


def test_load_module():
    # from res.flows import is_callable

    m1 = res.flows.get_node_module("flow_node_example")

    for op in ["handler", "generator", "reducer"]:
        assert op in list(m1.keys()), f"op {op} missing from module keys"

    m1 = res.flows.get_node_module("flow_node_example.handler")
    keys = list(m1.keys())
    mod = m1[keys[0]]

    assert (
        len(keys) == 1
    ), "there should only be one key when loading the direct callable"

    assert callable(mod), "the directly called method is not callable"
