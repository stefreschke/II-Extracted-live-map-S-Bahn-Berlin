import sqlite3
import pandas as pd
import os


def write_dataframe_to_sql(filename="s-bahn-livekarte.db", data_frame=None):
    with sqlite3.connect('../data/' + filename) as conn:
        assert isinstance(data_frame, pd.DataFrame)
        data_frame.to_sql(name="trains", con=conn)


def read_data_frames():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_name = dir_path + "\\..\\data"
    directory = os.fsencode(dir_name)
    my_fancy_df = None
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.startswith("frame") and filename.endswith(".pkl"):
            df = pd.read_pickle(dir_name + "\\" + filename)
            my_fancy_df = pd.concat([my_fancy_df, df] if my_fancy_df is not None else [df],
                                    sort=False)
        else:
            continue
    my_fancy_df.to_pickle("../data/whole.pkl")


def read_data_frame_and_convert(data_frame="../data/whole.pkl"):
    df = pd.read_pickle(data_frame)
    write_dataframe_to_sql(data_frame=df)

