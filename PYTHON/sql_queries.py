import oracledb as odb
from sqlalchemy import create_engine
from sqlalchemy.dialects import oracle
import pandas as pd
from pw_secrets import password
import datetime


class SQLQuery:
    user = "inf3285"
    host = "studidb.gm.th-koeln.de"
    port = 1521
    sid = "vlesung"
    password = password

    def __init__(self):
        dsn_string = odb.makedsn(self.host, self.port, self.sid)

        connection = odb.connect(user=self.user, password=self.password, dsn=dsn_string)
        print(connection)

        self.cursor = connection.cursor()

    def execute_query(self, operation, direction, station_code, start_date, end_date):
        start_date = start_date.strftime("%d-%b-%Y")
        end_date = end_date.strftime("%d-%b-%Y")
        operation_lookup = {"mean": "AVG", "sum": "SUM", "median": "MEDIAN"}
        if station_code == "":
            return self.on_direction(operation_lookup[operation], direction, start_date, end_date)
        return self.on_station(operation_lookup[operation], station_code, start_date, end_date)

    def on_station(self, operation, station_code, start_date, end_date):
        print(station_code)
        query = """ SELECT {operation}("max. temperature in the previous hour (°c)") FROM DATA_0
                    INNER JOIN WEATHER_STATION WS on DATA_0.STATION_CODE = WS.WID
                    WHERE WS.WNAME = '{station_code}' 
                    and DATA_0.TIMESTAMP BETWEEN TO_TIMESTAMP('{start_date}') 
                    and TO_TIMESTAMP('{end_date}')""".format(operation=operation,
                                                             station_code=station_code,
                                                             start_date=start_date,
                                                             end_date=end_date)
        data = []
        for row in self.cursor.execute(query):
            data.append(row)
        print(data)
        return data

    def on_direction(self, operation, direction, start_date, end_date):
        query = """ SELECT {operation}("max. temperature in the previous hour (°c)") FROM DATA_0
                    INNER JOIN WEATHER_STATION WS on DATA_0.STATION_CODE = WS.WID
                    INNER JOIN DIRECTIONS D on D.DID = WS.DID
                    WHERE D.NAME = '{direction}' 
                    and DATA_0.TIMESTAMP BETWEEN TO_TIMESTAMP('{start_date}') 
                    and TO_TIMESTAMP('{end_date}')""".format(operation=operation,
                                                             direction=direction,
                                                             start_date=start_date,
                                                             end_date=end_date)
        data = []
        for row in self.cursor.execute(query):
            data.append(row)
        print(data)
        return data
