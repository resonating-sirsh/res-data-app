from res.utils.dataframes import remap_df_to_dicts
import pandas as pd
import numpy as np


def test_remap_df_to_dicts():
    df = pd.DataFrame({"ONE": np.zeros(3), "TWO": np.zeros(3), "THREE": np.zeros(3)})

    mapping = {
        "ONE": "one",
        "TWO": {"two": lambda x: x + 1},
        "THREE": "metadata__three",
    }

    dicts = remap_df_to_dicts(df, mapping)

    assert dicts == [
        {"one": 0.0, "two": 1.0, "metadata": {"three": 0.0}},
        {"one": 0.0, "two": 1.0, "metadata": {"three": 0.0}},
        {"one": 0.0, "two": 1.0, "metadata": {"three": 0.0}},
    ]
