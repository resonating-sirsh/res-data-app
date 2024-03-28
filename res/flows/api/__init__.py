from .FlowAPI import FlowAPI

_API = FlowAPI


def simple_mutation_arg_inspector(path):
    try:

        def c(x):
            return x.split(":")[0].split("$")[-1]

        with open(path) as f:
            d = f.read()
        return [c(x) for x in d[d.index("(") : d.index(")")][1:-1].split(",")]
    except:
        return None
