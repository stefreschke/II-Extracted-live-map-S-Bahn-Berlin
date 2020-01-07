"""
Cleaning step as turned in on 08.01.2020.
"""
import sqlite3
import file_resources
import pandas as pd
import datetime

DAYS_TO_KEEP_WEATHER_BEFORE_AFTER_SNAPSHOTS = 7 #keep 1 week before and after last snapshot

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

def remove_weather_out_of_range():
    with sqlite3.connect(file_resources.INTEGRATED_DB_FILE) as conn:
        cursor = conn.cursor()
        row = cursor.execute('SELECT Min(date), max(date) From snapshot').fetchone()

        minDate = datetime.datetime.strptime(row[0], '%Y-%m-%d') - datetime.timedelta(days=DAYS_TO_KEEP_WEATHER_BEFORE_AFTER_SNAPSHOTS)
        maxDate = datetime.datetime.strptime(row[1], '%Y-%m-%d') + datetime.timedelta(days=DAYS_TO_KEEP_WEATHER_BEFORE_AFTER_SNAPSHOTS)
        
        cursor.execute('DELETE From weather Where at_time < ?', (minDate.strftime("%Y-%m-%d %H:%M:%S"),))
        aff_rows = cursor.rowcount

        cursor.execute('DELETE From weather Where at_time > ?', (maxDate.strftime("%Y-%m-%d %H:%M:%S"),))
        aff_rows += cursor.rowcount
        
        print("Deleted %d unnecessary rows in weather table (out of snapshot datetime-range)" % aff_rows)

        #11323/13171 rows deleted
        #size before: 1.331.200 bytes
        #size after: 548.864 bytes

if __name__ == '__main__':
    delays, snapshots, candidates = get_integrated_db(all_data_frames)
    remove_weather_out_of_range()

