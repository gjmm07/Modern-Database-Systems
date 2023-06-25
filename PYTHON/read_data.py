import numpy as np
import pandas as pd
import os
from datetime import datetime as dt
import shutil

PATH = "/home/finn/Dokumente/DigitalScienceMaster/ModernDatabaseSystems/archive/"


def read_single_w_station(filename: str, station_id: str, columns):
    rows_to_keep = set(np.where(pd.read_csv(PATH + "data/{}.csv".format(filename),
                                            usecols=["station_code"]) == station_id)[0] + 1)
    num_lines = sum(1 for _ in open(PATH + "data/{}.csv".format(filename)))
    rows_to_exclude = set(range(num_lines)) - rows_to_keep
    df = pd.read_csv(PATH + "data/{}.csv".format(filename),
                     skiprows=rows_to_exclude, header=None)
    df.drop(0, axis=1, inplace=True)
    df.columns = columns
    df["timestamp"] = pd.to_datetime((df["date"] + "-" + df["hour"]), format="%Y-%m-%d-%H:%M")
    df.set_index("timestamp", inplace=True)
    df.drop(["date", "hour"], axis=1, inplace=True)
    df.sort_index(inplace=True)
    df.replace(-9999, np.nan, inplace=True)
    return df


def return_station(filename: str):
    np.random.seed(5)
    ids = np.unique(pd.read_csv(PATH + "data/{}.csv".format(filename),
                                usecols=["station_code"]))
    np.random.shuffle(ids)
    for i in ids:
        yield i


def process_df(dfs):
    directions = [key for key, value in dfs.items() for _ in range(len(value))]
    df = pd.concat([x for df in dfs.values() for x in df])
    df = df[["station_code", "max. temperature in the previous hour (°c)"]]
    station_code = df.drop_duplicates(subset="station_code")["station_code"].reset_index().drop("timestamp", axis=1)
    station_code["directions"] = directions
    df["station_code"] = pd.factorize(df.station_code)[0] + 1
    return df, station_code


def process_df_influxdb(data):
    dd = []
    for key, dfs in data.items():
        for df in dfs:
            df["region"] = key
            dd.append(df[["station_code", "max. temperature in the previous hour (°c)", "region"]])
    df = pd.concat(dd)
    df.sort_index(inplace=True)
    df.reset_index(inplace=True)
    print(df)
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df.rename(columns={"timestamp": "_time", "max. temperature in the previous hour (°c)": "_value"}, inplace=True)
    df["_measurement"] = "°C"
    df["_field"] = "temperature"
    df = df[["_time", "_value", "_field", "_measurement", "station_code", "region"]]
    return df, None


def save_file(*files, db_type="sql"):
    print(files)
    columns = pd.read_csv(PATH + "columns_description.csv")["columns_en"]
    dfs = {key[0]: [] for key in files}
    for file, amount in files:
        gen = return_station(file)
        for _ in range(amount):
            station_name = next(gen)
            print(station_name)
            df = read_single_w_station(file, station_name, columns)
            dfs[file] += [df]
    df, station_code = None, None
    if db_type == "sql":
        df, station_code = process_df(dfs)
    elif db_type == "timeseries":
        df, station_code = process_df_influxdb(dfs)
    df.dropna(inplace=True)
    i = 0
    path = "Preprocessed_data/{db_type}_data_{i}.csv".format(db_type=db_type, i=i)
    while os.path.exists(path):
        i += 1
        path = "Preprocessed_data/{db_type}_data_{i}.csv".format(db_type=db_type, i=i)
    if db_type == "sql":
        df.to_csv(path)
        station_code.to_csv("Preprocessed_data/{db_type}_stations_{i}.csv".format(db_type=db_type, i=i))
    elif db_type == "timeseries":
        # shutil.copy2("Preprocessed_data/header_timeseries.csv", path)
        df.to_csv(path)


if __name__ == "__main__":
    # save_file(("central_west", 2), ("north", 2), ("south", 2), ("northeast", 2), ("southeast", 2), db_type="timeseries")
    df = read_single_w_station("central_west", "A719", pd.read_csv(PATH + "columns_description.csv")["columns_en"])
    df2 = read_single_w_station("central_west", "A724", pd.read_csv(PATH + "columns_description.csv")["columns_en"])
    df3 = pd.concat((df, df2))
    print(np.nanmean((df3["max. temperature in the previous hour (°c)"].to_numpy())))
    # A719 26.013913282376837 (no problems)
    # A443 24.792159923845787 (sql with problems)
    # A748 23.819332592945155 (sql with problems)
