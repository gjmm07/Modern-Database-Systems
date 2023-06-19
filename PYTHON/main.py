import numpy as np
import pandas as pd
import tkinter as tk
from sql_queries import SQLQuery
import datetime as dt
from influxdb_queries import InfluxDBQuery


class Window(tk.Frame):

    def __init__(self, window):
        super().__init__()
        self.window = window

        self.sql_handler = SQLQuery()
        self.timeseries_handler = InfluxDBQuery()

        self.executing_type = tk.StringVar(master=self.window, value="mean")
        tk.OptionMenu(self.window, self.executing_type, "mean", "sum", "median").grid(row=0, column=0)

        self.database_type = tk.StringVar(master=self.window, value="sql")
        tk.OptionMenu(self.window, self.database_type, "sql", "timeseries").grid(row=1, column=0)

        df = pd.read_csv("Preprocessed_data/sql_stations_0.csv")
        stations = df["station_code"].to_numpy()
        directions = df["directions"].to_numpy()

        self.directions = tk.StringVar(master=self.window)
        tk.OptionMenu(self.window, self.directions, *np.unique(directions), "").grid(row=2, column=0)

        self.station = tk.StringVar(self.window)
        self.station_options = tk.OptionMenu(self.window,
                                             self.station,
                                             *stations, "")
        self.station_options.grid(row=3, column=0)
        self.station_options.config(width=20)

        self.days_start = tk.StringVar(master=self.window, value="1")
        self.months_start = tk.StringVar(master=self.window, value="1")
        self.years_start = tk.StringVar(master=self.window, value="2000")

        self.days_end = tk.StringVar(master=self.window, value="1")
        self.months_end = tk.StringVar(master=self.window, value="1")
        self.years_end = tk.StringVar(master=self.window, value="2022")
        tk.OptionMenu(self.window, self.days_start, *list(range(1, 32))).grid(row=0, column=3)
        tk.OptionMenu(self.window, self.months_start, *list(range(1, 13))).grid(row=0, column=4)
        tk.OptionMenu(self.window, self.years_start, *list(range(2000, 2023))).grid(row=0, column=5)
        tk.OptionMenu(self.window, self.days_end, *list(range(1, 32))).grid(row=1, column=3)
        tk.OptionMenu(self.window, self.months_end, *list(range(1, 13))).grid(row=1, column=4)
        tk.OptionMenu(self.window, self.years_end, *list(range(2000, 2023))).grid(row=1, column=5)

        self.grouper = tk.StringVar(master=self.window, value="Fully")
        tk.OptionMenu(self.window, self.grouper, "Fully", "Years", "Quarters", "Months", "Weeks").grid(row=2, column=3)

        self.execute_button = tk.Button(text="execute", master=self.window, command=self.execute)
        self.execute_button.grid(row=6, column=0)

        self.result_text = tk.StringVar(master=self.window)
        result_label = tk.Label(self.window, textvariable=self.result_text, relief="solid")
        result_label.config(width=30, height=20)
        result_label.grid(row=6, column=1)
        self.exit_button = tk.Button(text="exit", master=self.window, command=self.exit)
        self.exit_button.grid(row=6, column=4)

    def execute(self):
        # todo: check for errors e.g. start date smaller than end date
        start_date = dt.date(int(self.years_start.get()), int(self.months_start.get()), int(self.days_start.get()))
        end_date = dt.date(int(self.years_end.get()), int(self.months_end.get()), int(self.days_end.get()))
        if self.database_type.get() == "sql":
            result = self.sql_handler.execute_query(self.executing_type.get(),
                                                    self.directions.get(),
                                                    self.station.get(),
                                                    start_date,
                                                    end_date,
                                                    self.grouper.get())
            self.result_text.set(result)
        elif self.database_type.get() == "timeseries":
            result = self.timeseries_handler.execute_query(self.executing_type.get(),
                                                           self.directions.get(),
                                                           self.station.get(),
                                                           start_date,
                                                           end_date,
                                                           self.grouper.get())
            self.result_text.set(result)

    def exit(self):
        self.window.quit()


if __name__ == "__main__":
    root = tk.Tk()
    Window(root)
    root.mainloop()
