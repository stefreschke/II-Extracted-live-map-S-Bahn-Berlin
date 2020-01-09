"""
Cleaning step as turned in on 08.01.2020.
"""
import sqlite3
import file_resources
import pandas as pd
import datetime
from extract import create_data_bases as create_db

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


def build_day_table():
    delays, snapshots, candidates = get_integrated_db(all_data_frames)
    days, times = split_up_snapshots(snapshots)
    # vehicles = vehicle_table(delays)
    appended_delays = joined_delays(delays, snapshots)
    new_df = find_used_trains_for_day(appended_delays, days)
    write_day_table_to_db(new_df)


def write_day_table_to_db(df):
    with sqlite3.connect(file_resources.INTEGRATED_DB_FILE) as conn:
        create_db(conn, file_path="create_days_table.sql")
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                'INSERT INTO days (day, vehicle, first_seen, last_seen, delay) VALUES (?, ?, ?, '
                '?, ?)',
                (row["day"], row["vehicle"], row["first_seen"], row["last_seen"], row["delay"]))


def find_used_trains_for_day(appended_delays, days):
    result = pd.DataFrame(columns=['day', 'vehicle', 'first_seen', 'last_seen', 'delay'])
    for _, row in days.iterrows():
        day = row.date
        data_frame = appended_delays[appended_delays["date"] == day]
        data_frame = data_frame.sort_values("time")
        data_frame = data_frame[
            data_frame["time"].gt("07:59:00") & data_frame["time"].lt("09:00:00")]
        vehicles = find_vehicles_used_at_day(data_frame)
        vehicle_df = find_really_used_vehicle(data_frame, vehicles)
        vehicle = vehicle_df.iloc[0]["vehicle_id"]
        last_seen = vehicle_df.time.max()
        first_seen = vehicle_df.time.min()
        delay = delay_considering_entry_and_exit(first_seen, last_seen, vehicle_df)
        result = result.append(
            {'day': day, 'vehicle': vehicle, 'first_seen': first_seen, 'last_seen': last_seen,
             'delay': delay},
            ignore_index=True)
    return result


def delay_considering_entry_and_exit(first_seen, last_seen, vehicle_df):
    return vehicle_df[vehicle_df["time"] == last_seen].iloc[0].delay - vehicle_df[
        vehicle_df["time"] == first_seen].iloc[0].delay


def find_really_used_vehicle(data_frame, vehicles):
    min_x = float("inf")
    used_vehicle = None
    for vehicle in vehicles:
        vehicle_df = data_frame[data_frame["vehicle_id"] == vehicle]
        vehicle_df = vehicle_df.drop(columns=["snapshot", "date"])
        first_seen = vehicle_df.time.min()
        x_coordinate = vehicle_df[vehicle_df["time"] == first_seen].iloc[0].x
        if x_coordinate < min_x:
            used_vehicle = vehicle_df
    return used_vehicle


def find_vehicles_used_at_day(data_frame):
    first_vehicles = data_frame[data_frame["time"] == data_frame.iloc[0]["time"]]
    new_vehicles = data_frame[data_frame["time"] != data_frame.iloc[0]["time"]]
    first_vehicles = first_vehicles["vehicle_id"].unique()
    matches = None
    while matches is None:
        time = new_vehicles["time"].min()
        entries = new_vehicles[new_vehicles["time"] == time]
        new_vehicles = new_vehicles[new_vehicles["time"] != time]
        entries = entries[~entries["vehicle_id"].isin(first_vehicles)]
        if len(entries) > 0:
            matches = list(entries["vehicle_id"].unique())
        if len(new_vehicles) == 0:
            matches = []
    return matches


if __name__ == '__main__':
    remove_weather_out_of_range()
    build_day_table()
