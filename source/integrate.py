import sqlite3
import pandas as pd
import numpy as np
import geopy.distance
import file_resources
import extract_weather

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
    df["date"] = df.apply(lambda line: str(line["checkpoint"].date()), axis=1)
    df["time"] = df.apply(lambda line: str(line["checkpoint"].time()), axis=1)
    df.drop(columns=["checkpoint", "line"], inplace=True)
    df["x"] /= 1000000
    df["y"] /= 1000000
    return df


def calculate_distances_to_targets(df):
    map_griebnitzsee = lambda line: calculate_distance_to(line, griebnitzsee)
    map_berlin_hbf = lambda line: calculate_distance_to(line, berlin_hbf)
    df["distance_griebnitzsee"] = df.apply(map_griebnitzsee, axis=1)
    df["distance_berlin_hbf"] = df.apply(map_berlin_hbf, axis=1)
    return df


def calculate_distance_to(line, coordinates):
    return geopy.distance.vincenty((line["y"], line["x"]),
                                   (coordinates["y"], coordinates["x"])).km


def write_table_candidates(df):
    with sqlite3.connect(file_resources.INTEGRATED_DB_FILE) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS "candidate_trains" (
                "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "day" TEXT NOT NULL,
                "vehicle" TEXT NOT NULL
            );""")
        cursor = conn.cursor()

        for index, row in df.iterrows():
            cursor.execute('INSERT INTO candidate_trains (day, vehicle) VALUES (?,?)',
                           (row["date"], row["vehicle_id"]))


def write_list_of_snapshots(df):
    with sqlite3.connect(file_resources.INTEGRATED_DB_FILE) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS "snapshot" (
                "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "date" TEXT NOT NULL,
                "time" TEXT NOT NULL
            );""")
        cursor = conn.cursor()

        for index, row in df.iterrows():
            cursor.execute('INSERT INTO snapshot (date, time) VALUES (?,?)',
                           (row["date"], row["time"]))


def integrate():
    df = reduced_s_bahn()
    df = calculate_distances_to_targets(df)
    # df = df.query("x<{} and x>{}".format(berlin_hbf["x"], griebnitzsee["x"]))
    df = df.loc[(df['x'] >= griebnitzsee["x"]) & (df['x'] <= berlin_hbf["x"])]
    df = df.loc[df["destination"] == "S Potsdam Hauptbahnhof"]

    candidates = df[["date", "vehicle_id"]]
    candidates.drop_duplicates(inplace=True)
    candidates = candidates.reset_index(drop=True)

    snapshots = df[["date", "time"]]
    snapshots = snapshots.drop_duplicates()
    snapshots = snapshots.reset_index(drop=True)

    # df["snapshot"] = df.apply(axis=1)

    write_table_candidates(candidates)
    write_list_of_snapshots(snapshots)
    pass


if __name__ == '__main__':
    extract_weather.extract_weather()
    integrate()
    pass
