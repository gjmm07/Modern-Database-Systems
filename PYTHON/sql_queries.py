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

    def execute_query(self, operation, direction, station_code, start_date, end_date, grouper):
        print(grouper)
        start_date = start_date.strftime("%d-%b-%y")
        end_date = end_date.strftime("%d-%b-%y")
        operation_lookup = {"mean": "AVG", "sum": "SUM", "median": "MEDIAN"}
        if station_code == "":
            print("____" + grouper)
            if grouper != "Fully":
                query = self.on_direction_grouper(operation_lookup[operation], direction, start_date, end_date, grouper)
            else:
                query = self.on_direction(operation_lookup[operation], direction, start_date, end_date)
        else:
            if grouper != "Fully":
                query = self.on_station_grouper(operation_lookup[operation], station_code, start_date,
                                                end_date, grouper)
            else:
                query = self.on_station(operation_lookup[operation], station_code, start_date, end_date)
        data = []
        for row in self.cursor.execute(query):
            data.append(row)
        try:
            return "\n".join([str(item[0:-1]) + ":" + "%.2f" % item[-1] for item in data])
        except IndexError:
            return "%.2f" % data[0][0]

    @staticmethod
    def on_station(operation, station_code, start_date, end_date):
        query = """ SELECT {operation}("max. temperature in the previous hour (°c)") FROM SQL_DATA_0
                    INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
                    WHERE WS.WNAME = '{station_code}' 
                    and SQL_DATA_0.TIMESTAMP BETWEEN TO_TIMESTAMP('{start_date}') 
                    and TO_TIMESTAMP('{end_date}')""".format(operation=operation,
                                                             station_code=station_code,
                                                             start_date=start_date,
                                                             end_date=end_date)
        return query

    @staticmethod
    def on_direction(operation, direction, start_date, end_date):
        query = """ SELECT {operation}("max. temperature in the previous hour (°c)") FROM SQL_DATA_0
                    INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
                    INNER JOIN DIRECTIONS D on D.DID = WS.DID
                    WHERE D.NAME = '{direction}' 
                    and SQL_DATA_0.TIMESTAMP BETWEEN TO_TIMESTAMP('{start_date}') 
                    and TO_TIMESTAMP('{end_date}')""".format(operation=operation,
                                                             direction=direction,
                                                             start_date=start_date,
                                                             end_date=end_date)
        return query

    def get_query_args(self, grouper):
        base = "extract(year from TIMESTAMP)"
        extractor = "{} as yr".format(base)
        order_query = "ORDER BY yr"
        grouper_query = "GROUP BY {}".format(base)
        if grouper == "Months":
            base = "extract(month from TIMESTAMP)"
            extractor += ", {} as m".format(base)
            grouper_query += ", {}".format(base)
            order_query += ", m"
        return extractor, order_query, grouper_query

    def on_station_grouper(self, operation, station_code, start_date, end_date, grouper):
        extractor, order_query, grouper_query = self.get_query_args(grouper)
        query = """ SELECT {extractor}, {operation}("max. temperature in the previous hour (°c)") FROM SQL_DATA_0
                    INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
                    WHERE WS.WNAME = '{station_code}' AND TIMESTAMP BETWEEN TO_TIMESTAMP('{start_date}') and TO_TIMESTAMP('{end_date}')
                    {grouper_query}
                    {order_query}""".format(extractor=extractor,
                                            operation=operation,
                                            station_code=station_code,
                                            start_date=start_date,
                                            end_date=end_date,
                                            grouper_query=grouper_query,
                                            order_query=order_query)
        return query

    def on_direction_grouper(self, operation, direction, start_date, end_date, grouper):
        extractor, order_query, grouper_query = self.get_query_args(grouper)
        query = """ SELECT {extractor}, {operation}("max. temperature in the previous hour (°c)") FROM SQL_DATA_0
                            INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
                            INNER JOIN DIRECTIONS D on D.DID = WS.DID
                            WHERE D.NAME = '{direction}' AND TIMESTAMP BETWEEN TO_TIMESTAMP('{start_date}') and TO_TIMESTAMP('{end_date}')
                            {grouper_query}
                            {order_query}""".format(extractor=extractor,
                                                    operation=operation,
                                                    direction=direction,
                                                    start_date=start_date,
                                                    end_date=end_date,
                                                    grouper_query=grouper_query,
                                                    order_query=order_query)
        print(query)
        return query


