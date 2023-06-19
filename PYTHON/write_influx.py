import influxdb_client
import pandas as pd
from influxdb_client.client.write_api import SYNCHRONOUS

# restarting influxdb if port is occupied
# in linux terminal
# sudo netstat -lptn | grep influx
# sud kill <porcess id>

# increasing swap memory
# sudo swapoff -a
# sudo dd if=/dev/zero of=/swapfile bs=1G count=2
# sudo chmod 0600 /swapfile
# sudo mkswap /swapfile
# sudo swapon /swapfile
# https://askubuntu.com/questions/178712/how-to-increase-swap-space



bucket = "temp_brazil"
org = "TH Köln"
token = "wDiaZebrtFetICbWC-rcsHi0RjX9FVQkXi3hdmlzobLDp9UEW4MyT-_uy54dlgufolr1qa1_JOfIKJtjIiJRTA=="
url = "http://localhost:8086"

client = influxdb_client.InfluxDBClient(url=url,
                                        token=token,
                                        org=org)

write_api = client.write_api(write_options=SYNCHRONOUS)


def read_df_chucks(chuck_size=1000):
    rows = sum(1 for _ in open("Preprocessed_data/timeseries_data_0.csv", encoding="utf-8"))
    start = 0
    for i in range(chuck_size, rows, chuck_size):
        yield pd.read_csv("Preprocessed_data/timeseries_data_0.csv",
                          skiprows=set(range(1, rows)) - set(range(start, i)), index_col=0)
        start = i
    yield pd.read_csv("Preprocessed_data/timeseries_data_0.csv",
                      skiprows=set(range(1, rows)) - set(range(i, rows)))


def write_influx2(row):
    p = influxdb_client.Point("my measurement")\
        .tag("region", row["region"])\
        .field("temperature", row["_value"])\
        .time(row["_time"])\
        .tag("station_code", row["station_code"])
    write_api.write(bucket=bucket, org=org, record=p)


counter = 0
for subframe in read_df_chucks():
    subframe.apply(write_influx2, axis=1)
    print(counter)
    counter += 1




