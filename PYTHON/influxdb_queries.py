import influxdb_client
from datetime import datetime as dt


class InfluxDBQuery:
    token = "wDiaZebrtFetICbWC-rcsHi0RjX9FVQkXi3hdmlzobLDp9UEW4MyT-_uy54dlgufolr1qa1_JOfIKJtjIiJRTA=="
    org = "TH Köln"
    url = "http://localhost:8086"

    def __init__(self):

        self.client = influxdb_client.InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org
        )
        self.query_api = self.client.query_api()

    def execute_query(self, operation, direction, station_code, start_date, end_date, grouper):
        influx_groups = {"Fully": None, "Years": "1y", "Quarters": "3mo", "Months": "1mo", "Weeks": "w"}
        start_date = start_date.strftime("%Y-%m-%dT00:00:00Z")
        end_date = end_date.strftime("%Y-%m-%dT00:00:00Z")
        operation_lookup = {"mean": "mean", "sum": "sum", "median": "median"}
        if station_code == "":
            query = self.on_direction(operation_lookup[operation], direction, start_date,
                                      end_date, influx_groups[grouper])
        else:
            query = self.on_station(operation_lookup[operation], station_code, start_date,
                                    end_date, influx_groups[grouper])
        print(query)
        tables = self.query_api.query(query, org=self.org)
        data = []
        for table in tables:
            for record in table.records:
                data.append(record.get_start().strftime("%Y-%m-%d") + " " +
                            record.get_stop().strftime("%Y-%m-%d") + " " + "%.2f" % record.get_value())

        return "\n".join(data)

    @staticmethod
    def on_direction(operation, direction, start_date, end_date, grouper):
        window = "|> window(every: {grouper})".format(grouper=grouper) if grouper is not None else ""
        print(window)
        query = """from(bucket: "temp_brazil")
        |> range(start: {start_date}, stop: {end_date})
        |> filter(fn: (r) => r.region == "{direction}")
        |> group(columns: ["region", "_measurement"], mode:"by")
        {window}
        |> {operation}()""".format(start_date=start_date,
                                   end_date=end_date,
                                   direction=direction,
                                   window=window,
                                   operation=operation)
        return query

    @staticmethod
    def on_station(operation, station_code, start_date, end_date, grouper):
        window = "|> window(every: {grouper})".format(grouper=grouper) if grouper is not None else ""
        query = """from(bucket: "temp_brazil")
        |> range(start: {start_date}, stop: {end_date})
        |> filter(fn: (r) => r.station_code == "{station_code}")
        {window}
        |> {operation}(column: "_value")""".format(start_date=start_date,
                                                   end_date=end_date,
                                                   station_code=station_code,
                                                   window=window,
                                                   operation=operation)
        return query
