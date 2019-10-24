"""
Converting stuff taken from json files to better form.
"""
import pandas as pd


def convert_sub_data_frame(data_frame):
    """
    Take a pandas dataframe of trains/busses (at a given time, from a given tracked state).
    :param data_frame: Input dataframe (of strings)
    :return: Nicer, less memeory consuming dataframe
    """
    data_frame.drop(columns=["p"], inplace=True)
    data_frame.x = pd.to_numeric(data_frame["x"])
    data_frame.y = pd.to_numeric(data_frame["y"])
    # df[["x", "y"]] = df[["x", "y"]] / 10e5
    data_frame["n"] = data_frame["n"].astype("category")
    data_frame["rd"] = data_frame["rd"].astype("category")
    data_frame[["c", "d"]] = data_frame[["c", "d"]].astype("int8")
    return data_frame
