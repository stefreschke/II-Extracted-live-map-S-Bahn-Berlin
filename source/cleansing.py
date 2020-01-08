"""
Cleaning step as turned in on 08.01.2020.
"""
import sqlite3
import file_resources
import pandas as pd
import datetime

DAYS_TO_KEEP_WEATHER_BEFORE_AFTER_SNAPSHOTS = 7  # keep 1 week before and after last snapshot


def get_integrated_db(do_something_with):
    with sqlite3.connect(file_resources.INTEGRATED_DB_FILE) as conn:
        return do_something_with(conn)


def all_data_frames(fancy_db):
    delays = read_table_from_db(fancy_db, "delays")
    snapshots = read_table_from_db(fancy_db, "snapshot")
    candidate_trains = read_table_from_db(fancy_db, "candidate_trains")
    return delays, snapshots, candidate_trains


def trains_for_each_day(delays, snapshots):
    snapshots.date = pd.to_datetime(snapshots.date)
    snapshots.set_index("id")
    delays.set_index("id")
    delays.join(snapshots)


def split_up_snapshots(snapshots):
    days = pd.DataFrame()
    days["date"] = snapshots.date.unique()
    days.reset_index()

    times = pd.DataFrame()
    times["time"] = snapshots.time.unique()
    days.reset_index()
    # for index, row in days.iterrows():
    #     df = snapshots[snapshots.date == row["date"]]
    #     df["date"] = index
    #     times = pd.concat([times, df], ignore_index=True)
    return days, times


def joined_delays(delays, snapshots):
    joined = pd.merge(left=delays, right=snapshots, left_on="snapshot", right_on="id").drop(
        columns=["id_x", "id_y"])
    return joined


def read_table_from_db(fancy_db, table):
    return pd.read_sql_query("SELECT * FROM " + table, fancy_db)


def vehicle_table(delays):
    vehicles = delays.vehicle_id.unique()
    vehicle_df = pd.DataFrame()
    for vehicle in vehicles:
        entries = delays[delays.vehicle_id == vehicle]
        number_of_entries = len(entries)
        vehicle_df = vehicle_df.append({'vehicle': vehicle, 'appearances': number_of_entries},
                                       ignore_index=True)
    return vehicle_df


def remove_weather_out_of_range():
    with sqlite3.connect(file_resources.INTEGRATED_DB_FILE) as conn:
        cursor = conn.cursor()
        row = cursor.execute('SELECT Min(date), max(date) From snapshot').fetchone()

        minDate = datetime.datetime.strptime(row[0], '%Y-%m-%d') - datetime.timedelta(
            days=DAYS_TO_KEEP_WEATHER_BEFORE_AFTER_SNAPSHOTS)
        maxDate = datetime.datetime.strptime(row[1], '%Y-%m-%d') + datetime.timedelta(
            days=DAYS_TO_KEEP_WEATHER_BEFORE_AFTER_SNAPSHOTS)

        cursor.execute('DELETE From weather Where at_time < ?',
                       (minDate.strftime("%Y-%m-%d %H:%M:%S"),))
        aff_rows = cursor.rowcount

        cursor.execute('DELETE From weather Where at_time > ?',
                       (maxDate.strftime("%Y-%m-%d %H:%M:%S"),))
        aff_rows += cursor.rowcount

        print(
            "Deleted %d unnecessary rows in weather table (out of snapshot datetime-range)" %
            aff_rows)

        # 11323/13171 rows deleted
        # size before: 1.331.200 bytes
        # size after: 548.864 bytes


if __name__ == '__main__':
    remove_weather_out_of_range()
    delays, snapshots, candidates = get_integrated_db(all_data_frames)
    days, times = split_up_snapshots(snapshots)
    vehicles = vehicle_table(delays)
    appended_delays = joined_delays(delays, snapshots)
