"""
Converting stuff taken from json files to better form.
"""
import pandas as pd
from pandas.io.json import json_normalize


def convert_sub_data_frame(data_frame, projections=None):
    """
    Take a pandas dataframe of trains/busses (at a given time, from a given tracked state).
    :param data_frame: Input dataframe (of strings)
    :return: Nicer, less memeory consuming dataframe
    """
    assert isinstance(data_frame, pd.DataFrame)
    assert isinstance(projections, pd.DataFrame)
    data_frame.drop(columns=["p"], inplace=True)
    for index, row in data_frame.iterrows():
        df = json_normalize(row["p"])
        df["datensatz"] = df.apply(lambda x: index, axis=1)
        projections = pd.concat([projections, df] if projections is not None else [df])
    data_frame.x = pd.to_numeric(data_frame["x"])
    data_frame.y = pd.to_numeric(data_frame["y"])
    # df[["x", "y"]] = df[["x", "y"]] / 10e5
    data_frame["n"] = data_frame["n"].astype("category").apply(lambda x: x.strip())
    data_frame["rd"] = data_frame["rd"].astype("category")
    data_frame[["c", "d"]] = data_frame[["c", "d"]].astype("int8")
    return data_frame, projections



