"""
Cleaning step as turned in on 08.01.2020.
"""
import sqlite3
import file_resources
import pandas as pd


def get_integrated_db(do_something_with):
    with sqlite3.connect(file_resources.INTEGRATED_DB_FILE) as conn:
        return do_something_with(conn)


def all_data_frames(fancy_db):
    delays = read_table_from_db(fancy_db, "delays")
    snapshots = read_table_from_db(fancy_db, "snapshot")
    candidate_trains = read_table_from_db(fancy_db, "candidate_trains")
    return delays, snapshots, candidate_trains


def read_table_from_db(fancy_db, table):
    return pd.read_sql_query("SELECT * FROM " + table, fancy_db)


if __name__ == '__main__':
    delays, snapshots, candidates = get_integrated_db(all_data_frames)
