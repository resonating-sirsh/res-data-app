import res
from ..main import app

app.testing = True


def test_full():
    res.flows.FlowContext.mocks = {}

    event = {}
    context = {}
