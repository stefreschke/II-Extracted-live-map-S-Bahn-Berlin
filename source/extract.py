"""
Performing a simple export from json data to sql, mostly without using deeper pandas features.
"""
import json
import os
import logging
import sqlite3
import pandas as pd

import file_resources
from project_logging import init_logger

LOGGER = logging.getLogger('extraction')


def main(sqlite_file=file_resources.SQLITE_FILE, data_file=file_resources.DATA_FILE,
         stops_file=file_resources.STOPS_FILE, limit=10e8):
    """
    Perform extraction for given files (which all default to values from file_resources.py).
    :param sqlite_file: Database to write stuff to.
    :param data_file: json-file to get all stuff from.
    :param stops_file: json-file containing data on stops.
    :param limit: db size file limit at which the extraction should be aborted.
    :return:
    """
    with sqlite3.connect(sqlite_file) as conn:
        # Create tables using sql file "create_tables.sql"
        create_data_bases(con=conn, file_path="create_tables.sql")
        cursor = conn.cursor()

        # big snapshot json file
        with open(data_file, encoding='utf-8') as file:
            for index, line in enumerate(file):
                parts = line.split("|")
                suspected_json = "|".join(parts[1:])
                try:
                    data_set = json.loads(suspected_json)
                except json.decoder.JSONDecodeError:
                    LOGGER.warning("Invalid line at %d", index)
                    continue
                at_time = parts[0].split(" ")[0] + " " + data_set["ts"]
                cursor.execute('INSERT INTO snapshot (taken_at) VALUES (?)', (at_time,))
                snapshot_id = cursor.lastrowid
                for entry in data_set["t"]:
                    cursor.execute(
                        'INSERT INTO datarecord(snapshot_id, x, y, n, l, i, rt, rd, d, c)'
                        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (
                            snapshot_id, entry["x"], entry["y"], entry["n"].strip(),
                            entry["l"].strip(), entry["i"].strip(),
                            entry["rt"] if "rt" in entry else 0, entry["rd"], entry["d"],
                            entry["c"]))
                    datarecord_id = cursor.lastrowid
                    for projections in entry["p"]:
                        cursor.execute(
                            'INSERT INTO projection(datarecord_id, x, y, t, d)'
                            ' VALUES (?, ?, ?, ?, ?)',
                            (datarecord_id, projections["x"], projections["y"], projections["t"],
                             projections["d"] if "d" in projections else None))
                # check if file size limit was reached
                if get_file_size(sqlite_file) / limit > 1:
                    break

        # Write stops to SQL-file as well
        stop_data_to_sql(con=conn, file_path=stops_file)
    LOGGER.info("Finished reading data from %s, wrote to %s", data_file, sqlite_file)


def stop_data_to_sql(con, file_path=file_resources.STOPS_FILE):
    """
    Write stops to given sql connection.
    :param con: SQL-connection that can be used to use to_sql on pd.DataFrame
    :param file_path: Path to stops.json (in data folder, e.g.)
    :return:
    """
    with open(file_path, encoding='utf-8') as file:
        stops = json.loads(file.read())
        data_frame = pd.DataFrame(stops["stops"])
        data_frame = data_frame.rename(columns=data_frame.iloc[0]).drop(data_frame.index[0])[
            ["x", "y", "name"]]
        data_frame.to_sql(name="stops", con=con, index_label="id")


def create_data_bases(con, file_path="create_tables.sql"):
    """
    Execute SQL-Script to generate tables. Script is written in another file to ensure code
    readability.
    :param con: sql-connection the tables should be created at
    :param file_path: to find sql-script at
    :return:
    """
    with open(file_path, 'r') as file:
        sql_script = file.read().replace('\n', '')
        con.executescript(sql_script)


def get_file_size(file_to_look_at):
    """
    Get file size using os.
    :param file_to_look_at: relative path to file to look at
    :return: file size in bytes (1 GB = 10e8 B)
    """
    return os.path.getsize(os.getcwd() + "\\" + file_to_look_at)


if __name__ == '__main__':
    init_logger()
    main()  # TODO: parse command line arguments to specify .db-file size limit
