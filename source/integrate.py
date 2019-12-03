import sqlite3
import pandas as pd
import numpy as np
import geopy.distance
import file_resources

griebnitzsee = {"x": 52.394444, "y": 13.127222}
berlin_hbf = {"x": 52.525, "y": 13.369444}


def reduced_s_bahn():
    traffic_db = sqlite3.connect(file_resources.TIMS_TRAFFIC_DB)
    df = pd.read_sql_query("SELECT * FROM delays AS d "
                           "WHERE d.line='S7' ", traffic_db)
    df.drop(columns=["d", "c"], inplace=True)
    df = df.astype({
        "x": int, "y": int, "delay": int,
        "line": str, "destination": str, "vehicle_id": str
    })
    df["checkpoint"] = pd.to_datetime(df["checkpoint"])
    df["dist_target"]
    df["x"] /= 1000000
    df["y"] /= 1000000
    return df


def calculate_distance_to(line, coordinates):
    return geopy.distance.vincenty((line["x"], line["y"]),
                                   (coordinates["x"], coordinates["y"])).km


if __name__ == '__main__':
    df = reduced_s_bahn()

    pass
