"""

"""
import pandas as pd


def convert_sub_dataframe(df):
    df.x = pd.to_numeric(df["x"])
    df.y = pd.to_numeric(df["y"])
    # df[["x", "y"]] = df[["x", "y"]] / 10e5
    df["n"] = df["n"].astype("category")
    df["rd"] = df["rd"].astype("category")
    df[["c", "d"]] = df[["c", "d"]].astype("int8")
    return df