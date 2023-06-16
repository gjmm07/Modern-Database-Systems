import influxdb_client


class InfluxDBQuery:
    token = "QfIzquDTn0BUC0Axt2NqTybYbtAe-y6l7R35BgdmfagH6tJIeYnMgDxDRftbMhRhokUybx9TetnmZiQA8Wgc-w=="
    org = "TH Köln"
    url = "http://localhost:8086"

    def __init__(self):

        self.client = influxdb_client.InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org
        )
        self.query_api = self.client.query_api()

    def execute_query(self, operation, direction, station_code, start_date, end_date):
        start_date = start_date.strftime("%Y-%m-%dT00:00:00Z")
        end_date = end_date.strftime("%Y-%m-%dT00:00:00Z")
        operation_lookup = {"mean": "mean", "sum": "sum", "median": "median"}
        if station_code == "":
            return self.on_direction(operation_lookup[operation], direction, start_date, end_date)
        return self.on_station(operation_lookup[operation], station_code, start_date, end_date)

    def on_direction(self, operation, direction, start_date, end_date):
        print("reached direction")
        query = """from(bucket: "real_test")
        |> range(start: {start_date}, stop: {end_date})
        |> filter(fn: (r) => r.region == "{direction}")
        |> {operation}(column: "_value")""".format(start_date=start_date,
                                                   end_date=end_date,
                                                   direction=direction,
                                                   operation=operation)
        print(query)
        tables = self.query_api.query(query, org=self.org)
        data = []
        for table in tables:
            for record in table.records:
                data.append(record.get_value())
                # print(record.get_field(), record.get_value())
        return data

    def on_station(self, operation, station_code, start_date, end_date):
        print("reached station")
        query = """from(bucket: "real_test")
        |> range(start: {start_date}, stop: {end_date})
        |> filter(fn: (r) => r.station == "{station_code}")
        |> {operation}(column: "_value")""".format(start_date=start_date,
                                                   end_date=end_date,
                                                   station_code=station_code,
                                                   operation=operation)
        print(query)

        tables = self.query_api.query(query, org=self.org)
        data = []
        for table in tables:
            for record in table.records:
                data.append(record.get_value())
                # print(record.get_field(), record.get_value())
        return data
