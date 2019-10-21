"""
Integration step for S Bahn Berlin data. Run this file to load and convert S Bahn data in nearby
files accordingly.
"""
import json
import pandas as pd
from pandas.io.json import json_normalize


def main():
    """
    Execution of integration step.
    :return:
    """
    with open('../data/traffic_data.json') as file:
        for index, line in enumerate(file):
            parts = line.split("|")
            timestamp = parts[0]
            suspected_json = "|".join(parts[1:])
            data_set = json.loads(suspected_json)
            entries = data_set["t"]
            df = json_normalize(entries)
            df.drop(columns=["p"], inplace=True)
            df.x = pd.to_numeric(df["x"])
            df.y = pd.to_numeric(df["y"])
            df[["x", "y"]] = df[["x", "y"]] / 10e5
            df["n"] = df["n"].astype("category")
            df[["c", "d"]] = df[["c", "d"]].astype("int8")
            df["rd"] = df["rd"].astype("category")


class JSONParseException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


if __name__ == '__main__':
    main()
