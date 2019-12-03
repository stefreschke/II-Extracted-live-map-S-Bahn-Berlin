"""
Extracts weather data from .csv's downloaded from DWD: Station 00403 / "Berlin-Dahlem (FU)"
"""
import res

import pandas as pd
from datetime import datetime
import sqlite3


def extract_weather():    
    df = pd.read_csv(res.TEMPERATURE_FILE, sep=';')
    df['MESS_DATUM'] = df['MESS_DATUM'].apply(lambda x : datetime.strptime(str(x), "%Y%m%d%H"))
    df = df[['MESS_DATUM', 'TT_TU']]
    df


    df2 = pd.read_csv(res.CLOUDINESS_FILE, sep=';')
    df2['MESS_DATUM'] = df2['MESS_DATUM'].apply(lambda x : datetime.strptime(str(x), "%Y%m%d%H"))
    df2[' V_N'] = df2[' V_N'].apply(lambda x : "clear" if x < 5 else "cloudy")
    df2 = df2[['MESS_DATUM', ' V_N']]
    df2

    df3 = pd.read_csv(res.RAIN_FILE, sep=';')
    df3['MESS_DATUM'] = df3['MESS_DATUM'].apply(lambda x : datetime.strptime(str(x), "%Y%m%d%H"))
    df3['RS_IND'] = df3['RS_IND'].apply(lambda x: "rainy" if x == 1 else "")
    df3 = df3[['MESS_DATUM','RS_IND']]
    df3


    dfall = df.merge(df2, on="MESS_DATUM").merge(df3, on="MESS_DATUM")
    dfall['RS_IND'] = dfall[[' V_N', 'RS_IND']].apply(lambda x: x[1] if x[1] != "" else x[0], axis=1)
    dfall = dfall[['MESS_DATUM', 'TT_TU', 'RS_IND']]
    dfall

    with sqlite3.connect(res.SQLITE_FILE) as conn:
        conn.executescript("""
            CREATE TABLE "weather" (
                "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "at_time"	TEXT NOT NULL UNIQUE,
                "temperature" REAL NOT NULL,
                "condition" TEXT NOT NULL
            );""")
        cursor = conn.cursor()

        for index, row in dfall.iterrows():
                cursor.execute('INSERT INTO weather (at_time, temperature, condition) VALUES (?,?,?)', (row['MESS_DATUM'].strftime("%Y-%m-%d %H:%M:%S"),row['TT_TU'],row['RS_IND']))


if __name__ == '__main__':
    extract_weather()