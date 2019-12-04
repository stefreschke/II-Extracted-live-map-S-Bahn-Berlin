import sqlite3
import pandas as pd
import numpy as np
import geopy.distance
import file_resources

griebnitzsee = {"y": 52.394444, "x": 13.127222}
berlin_hbf = {"y": 52.525, "x": 13.369444}


def reduced_s_bahn():
    traffic_db = sqlite3.connect(file_resources.TIMS_TRAFFIC_DB)
    df = pd.read_sql_query("SELECT * FROM delays AS d "
                           "WHERE d.line='S7' "
                           "AND TIME(d.checkpoint) BETWEEN '07:00:00' AND '10:00:00'", traffic_db)
    df.drop(columns=["d", "c"], inplace=True)
    df = df.astype({
        "x": int, "y": int, "delay": int,
        "line": str, "destination": str, "vehicle_id": str
    })
    df["checkpoint"] = pd.to_datetime(df["checkpoint"])
    df["x"] /= 1000000
    df["y"] /= 1000000
    return df


def calculate_distance_to(line, coordinates):
    return geopy.distance.vincenty((line["y"], line["x"]),
                                   (coordinates["y"], coordinates["x"])).km


if __name__ == '__main__':
    df = reduced_s_bahn()
    map_griebnitzsee = lambda line: calculate_distance_to(line, griebnitzsee)
    map_berlin_hbf = lambda line: calculate_distance_to(line, berlin_hbf)
    df["distance_griebnitzsee"] = df.apply(map_griebnitzsee, axis=1)
    df["distance_berlin_hbf"] = df.apply(map_berlin_hbf, axis=1)
    df = df.query("x<{} and x>{}".format(berlin_hbf["x"], griebnitzsee["x"]))
    pass
