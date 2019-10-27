"""
Performing a simple export from json data to sql, mostly without using deeper pandas features.
"""
import json
import logging
import sqlite3
import pandas as pd

import res
from log_stuff import init_logger

logger = logging.getLogger('extraction')


def main(sqlite_file=res.SQLITE_FILE, data_file=res.DATA_FILE, stops_file=res.STOPS_FILE):
    """
    Perform extraction for given files (which all default to values from res.py).
    :param sqlite_file: Database to write stuff to.
    :param data_file: json-file to get all stuff from.
    :param stops_file: json-file containing data on stops.
    :return:
    """
    with sqlite3.connect(sqlite_file) as conn:
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
        with open(data_file, encoding='utf-8') as file:
            for index, line in enumerate(file):
                parts = line.split("|")
                suspected_json = "|".join(parts[1:])
                try:
                    data_set = json.loads(suspected_json)
                except json.decoder.JSONDecodeError:
                    logger.warning("Invalid line at %d", index)
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
                if index > 3600:
                    break  # 1GB Limit by try out

        # stops
        with open(stops_file, encoding='utf-8') as file:
            stops = json.loads(file.read())
            df = pd.DataFrame(stops["stops"])
            df = df.rename(columns=df.iloc[0]).drop(df.index[0])[["x", "y", "name"]]
            df.to_sql(name="stops", con=conn, index_label="id")

    logger.info("Finished reading data from %s, wrote to %s", data_file, sqlite_file)


if __name__ == '__main__':
    init_logger()
    main()
