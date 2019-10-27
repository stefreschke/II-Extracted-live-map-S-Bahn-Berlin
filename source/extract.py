"""
Performing a simple export from json data to sql, mostly without using deeper pandas features.
"""
import json
import logging
import sqlite3
import pandas as pd

import res

SQLITE_FILENAME = '../data/' + "s-bahn-livekarte.db"


def main():
    with sqlite3.connect(res.SQLITE_FILE) as conn:
        conn.executescript("""
CREATE TABLE "snapshot" (
    "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "taken_at"	TEXT NOT NULL
);
CREATE TABLE "datarecord" (
    "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "snapshot_id"	INTEGER NOT NULL,
    "x"	INTEGER NOT NULL,
    "y"	INTEGER NOT NULL,
    "n"	TEXT NOT NULL,
    "l"	TEXT NOT NULL,
    "i"	TEXT NOT NULL,
    "rt"	INTEGER NOT NULL,
    "rd"	TEXT NOT NULL,
    "d"	INTEGER NOT NULL,
    "c"	INTEGER NOT NULL,
    FOREIGN KEY(snapshot_id) REFERENCES snapshot(id)
);
CREATE TABLE "projection" (
    "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "datarecord_id"	INTEGER NOT NULL,
    "x"	INTEGER NOT NULL,
    "y"	INTEGER NOT NULL,
    "t"	INTEGER NOT NULL,
    "d"	INTEGER,
    FOREIGN KEY(datarecord_id) REFERENCES datarecord(id)
);
        """)
        cursor = conn.cursor()

        # big snapshot json file
        with open(res.DATA_FILE, encoding='utf-8') as file:
            for index, line in enumerate(file):
                parts = line.split("|")
                suspected_json = "|".join(parts[1:])
                try:
                    data_set = json.loads(suspected_json)
                except json.decoder.JSONDecodeError:
                    print("Invalid line at {}".format(index))
                    continue
                at_time = parts[0].split(" ")[0] + " " + data_set["ts"]
                cursor.execute('INSERT INTO snapshot (taken_at) VALUES (?)', (at_time,))
                snapshot_id = cursor.lastrowid
                for entry in data_set["t"]:
                    cursor.execute(
                        'INSERT INTO datarecord(snapshot_id, x, y, n, l, i, rt, rd, d, c) VALUES '
                        '(?, '
                        '?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (
                        snapshot_id, entry["x"], entry["y"], entry["n"].strip(), entry["l"].strip(),
                        entry["i"].strip(), entry["rt"] if "rt" in entry else 0, entry["rd"],
                        entry["d"], entry["c"]))
                    datarecord_id = cursor.lastrowid
                    for p in entry["p"]:
                        cursor.execute(
                            'INSERT INTO projection(datarecord_id, x, y, t, d) VALUES (?, ?, ?, '
                            '?, ?)',
                            (datarecord_id, p["x"], p["y"], p["t"], p["d"] if "d" in p else None))
                if index > 3000:
                    break  # 1GB Limit

        # stops
        with open(res.STOPS_FILE, encoding='utf-8') as file:
            stops = json.loads(file.read())
            df = pd.DataFrame(stops["stops"])
            df = df.rename(columns=df.iloc[0]).drop(df.index[0])[["x", "y", "name"]]
            df.to_sql(name="stops", con=conn, index_label="id")

    print("Finished")


if __name__ == '__main__':
    main()
