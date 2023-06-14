import numpy as np
import pandas as pd
import os

PATH = "/home/finn/Dokumente/DigitalScienceMaster/ModernDatabaseSystems/archive/"


def read_single_w_station(filename: str, station_id: str, columns: str):
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


def save_file(*files):
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
    i = 0
    path = "Preprocessed_data/data_{}.csv".format(i)
    df, station_code = process_df(dfs)
    while os.path.exists(path):
        i += 1
        path = "Preprocessed_data/data_{}.csv".format(i)
    df.to_csv(path)
    station_code.to_csv("Preprocessed_data/stations_{}.csv".format(i))


if __name__ == "__main__":
    save_file(("central_west", 3), ("north", 3), ("south", 2), ("northeast", 2), ("southeast", 2))
    # save_file(("central_west", 2), ("north", 1))
    # save_file("northeast")
    # columns = pd.read_csv(PATH + "columns_description.csv")["columns_en"]
    # print(read_single_w_station("northeast", "A203", columns))

