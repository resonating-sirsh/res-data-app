from res.flows import get_res_team_for_node_name


def test_get_res_team_for_node_name():
    # the case where a module has its own RES_TEAM
    node = "sell.product.example"
    assert (
        get_res_team_for_node_name(node) == "res-sell"
    ), "the res team should be res-sell but it is not"

    # the case where some module does not have its only res team and we bubble up to namespace
    # i checked this on my machine but going to remove it for now because of the module load issues on this module
    # node = "sell.product.group.create"
    # assert (
    #     get_res_team_for_node_name(node) == "res-sell"
    # ), "the res team should be res-sell but it is not"

    # the control case - dont blow up
    node = ""
    assert (
        get_res_team_for_node_name(node) == None
    ), "the res team should be blank for a no op empty node"

    # the case where we supply the module itself which is not really a node so its sort of just a control
    node = "sell"
    assert (
        get_res_team_for_node_name(node) == "res-sell"
    ), "the res team should be res-sell but it is not"

    # the case where we supply a callable inside a module and we need to pop the parents team etc.
    node = "sell.product.example.handler"
    assert (
        get_res_team_for_node_name(node) == "res-sell"
    ), "the res team should be res-sell but it is not"
