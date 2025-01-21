from datetime import date
import pandas as pd
from  .data_mgt import df_to_cycle_table
from  utils import parse_cycle_to_db


def cycle_to_local_db(ifdb_read_dict, chamber_map):
    start = date(2024, 12, 5)
    end = date(2024, 12, 12)
    # cycles = parse_cycle_to_db(start, end, ifdb_read_dict, chamber_map)
    # df = cycles
    # NOTE: temporary logic for initiating cycle table
    df = pd.read_csv("/usr/src/app/ac_cycle_new.csv")
    df["start_time"] = pd.to_datetime(df["start_time"], format="%Y-%m-%d %H:%M:%S")
    df.dropna(subset="start_time", inplace=True)
    df.dropna(inplace=True)
    df.set_index("start_time", inplace=True)
    # df.tz_localize("Europe/Helsinki", ambiguous=True).tz_convert("UTC")
    df["start_time"] = df.index
    # df.drop_duplicates(subset="start_time", inplace=True)
    df.drop_duplicates(inplace=True)
    df_to_cycle_table(df)
