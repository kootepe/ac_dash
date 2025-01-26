import os
import json
import logging
import hashlib
import zipfile

from dash import ctx, no_update
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from plotly.graph_objs import Figure, Layout
from .db import engine

from .tools.influxdb_funcs import init_client, just_read

from .measuring import instruments
from .measurement import MeasurementCycle
from .data_mgt import (
    df_to_gas_table,
    gas_table_to_df,
    cycle_table_to_df,
    single_flux_to_table,
    flux_range_to_df,
    fluxes_to_table,
)
from .create_graph import (
    mk_attribute_plot,
    apply_highlighter,
)

logger = logging.getLogger("defaultLogger")
attribute_plots = {}
# track and save currently calculated measurements


protocol_pdargs = {
    "sep": "\t",
    "names": ["datetime", "id", "state"],
    "dtype": {
        "datetime": "str",
        "id": "str",
        "state": "str",
    },
    "usecols": ["datetime", "id", "state"],
}


def process_protocol_file(filedata, chamber_map):
    df = pd.read_csv(filedata, **protocol_pdargs)
    df["datetime"] = pd.to_datetime(df["datetime"], format="%d.%m.%Y %H:%M:%S")

    df["state"] = df["state"].astype(int)
    df["id"] = df["id"].astype(int)
    df.sort_values(by="datetime", inplace=True)

    df.sort_values(by="id").reset_index(drop=True, inplace=True)
    # NOTE: needs to be done in this seemingly overcomplicated way since
    # each cycle’s start time is the same (almost always) as end time of the previous cycle
    # so only grouping by datetime will cause issues since there's no
    # telling which order they'll end up
    # create list of dataframes where each dataframe only has cycles from
    # one chamber
    id_groups = [group for _, group in df.groupby("id")]
    grouped = []
    for id_group in id_groups:
        # calculate time differences between rows, and when time difference
        # is over 16minutes, increment. This creates a unique identifier for
        # each cycle
        id_group["cycle"] = (
            id_group["datetime"].diff() > pd.Timedelta(minutes=16)
        ).cumsum()
        # group each cycle into its own dataframe and append to list
        [grouped.append(group) for _, group in id_group.groupby("cycle")]
    #
    # sort the list of dataframes by the start time of the cycle
    sorts = sorted(grouped, key=lambda df: df["datetime"].iloc[0])

    dfs = []
    for df in sorts:
        if df.empty:
            continue
        # needs open, close and deactivate
        if df["state"].nunique() != 3:
            continue
        chamber = df["id"].iloc[0]
        # first ten is the start
        start = df.loc[df["state"] == 10, "datetime"].iloc[0]
        # first 11 is the close
        close = df.loc[df["state"] == 11, "datetime"].iloc[0]
        # second 10 should be the end, use -1 to grab the last to be
        # sure
        open = df.loc[df["state"] == 10, "datetime"].iloc[-1]
        # last 00 is the end
        end = df.loc[df["state"] == 0, "datetime"].iloc[-1]
        data = {
            "chamber_id": [chamber],
            "start_time": [start],
            "close_offset": [int((close - start).total_seconds())],
            "open_offset": [int((open - start).total_seconds())],
            "end_offset": [int((end - start).total_seconds())],
        }
        group = pd.DataFrame(data)
        # if any of these are negative there's something wrong
        if np.any(group[["close_offset", "open_offset", "end_offset"]].to_numpy() < 0):
            continue
        dfs.append(group)
    dfa = pd.concat(dfs)
    dfa["chamber_id"] = dfa["chamber_id"].astype(str).map(chamber_map)
    dfa["start_time"] = (
        dfa["start_time"]
        .dt.tz_localize("Europe/Helsinki", ambiguous=True)
        .dt.tz_convert("UTC")
    )

    return dfa


def process_protocol_zip(file_path, chamber_map):
    with zipfile.ZipFile(file_path, "r") as z:
        file_list = z.namelist()

        dfa = []
        for file_name in file_list:
            file_exts = "log"
            if file_name.endswith(file_exts):
                print(f"Processing: {file_name}")

                # open file and read to dataframe
                with z.open(file_name) as f:
                    df = process_protocol_file(f, chamber_map)
                    dfa.append(df)
    data = pd.concat(dfa)
    return data


def process_measurement_file(filedata, instrument):
    return instrument.read_output_file(filedata)


def process_measurement_zip(file_path, instrument):
    with zipfile.ZipFile(file_path, "r") as z:
        file_list = z.namelist()

        in_rows = 0
        push_rows = 0
        for file_name in file_list:
            file_exts = ("csv", "DATA", "DAT")
            if file_name.endswith(file_exts):
                print(f"Processing: {file_name}")

                # open file and read to dataframe
                with z.open(file_name) as f:
                    df = instrument.read_output_file(f)
                    df["datetime"] = (
                        df["datetime"]
                        .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                        .dt.tz_convert("UTC")
                    )
                    df["instrument_serial"] = instrument.serial
                    df["instrument_model"] = instrument.model
                    df_copy, dupes = df_to_gas_table(df)
                    in_rows += len(df)
                    push_rows += len(df_copy)
    return push_rows, in_rows


def load_config():
    """Load configuration for InfluxDB."""
    filepath = os.path.abspath(os.path.dirname(__file__))
    with open(f"{filepath}/config/config.json", "r") as f:
        defaults = json.load(f)
    with open(f"{filepath}/config/custom.json", "r") as f:
        custom = json.load(f)
    config = {**defaults, **custom}
    return (
        config["chambers"],
        config["chamber_map"],
        config["layout"],
        config["measurements"],
    )


def load_cycles():
    """Load measurement cycles from a configuration file."""
    with open("config/cycle.json", "r") as f:
        return json.load(f)["CYCLE"]


def generate_month():
    """Generate a list of the past 70 days (as dates)."""
    today = datetime.today()
    return [(today - timedelta(days=i)).date() for i in range(10)][::-1]


def generate_measurements(month, cycles):
    """Generate MeasurementCycle objects for each day and cycle."""
    all_measurements = []
    for day in month:
        for cycle in cycles:
            if pd.Timestamp(f"{day} {cycle.get('START')}") > datetime.now():
                continue
            s, c, o, e = (
                pd.Timestamp(f"{day} {cycle.get(time)}")
                for time in ["START", "CLOSE", "OPEN", "END"]
            )
            all_measurements.append(MeasurementCycle(cycle["CHAMBER"], s, c, o, e))
    return all_measurements


def init_from_cycle_table(cycle_df, use_class=None, serial=None, conn=None):
    instrument = instruments.get(use_class)(serial)
    all_measurements = []
    o = 0
    for idx, row in cycle_df.iterrows():
        o += 1
        start = row.start_time
        close = row.close_offset
        open = row.open_offset
        end = row.end_offset
        id = row.chamber_id
        logger.info(f"Initiating {start}")
        m = MeasurementCycle(id, start, close, open, end, instrument, conn=conn)
        if m.data is not None and not m.data.empty:
            # single_flux_to_table(m.attribute_df)
            all_measurements.append(m.attribute_df)
        if o == 100:
            o = 0
            if len(all_measurements) == 0:
                continue
            df = pd.concat(all_measurements)
            fluxes_to_table(df)
            all_measurements = []
            continue
    if len(all_measurements) > 0:
        df = pd.concat(all_measurements)
        fluxes_to_table(df)


def generate_measurements2(cycles, serial, use_class):
    """Generate MeasurementCycle objects for each day and cycle."""
    global measurements
    instrument = instruments.get(use_class)(serial)

    all_measurements = []
    with engine.connect() as conn:
        for cycle in cycles:
            s, c, o, e = (cycle.get(time) for time in ["START", "CLOSE", "OPEN", "END"])
            all_measurements.append(
                MeasurementCycle(cycle["CHAMBER"], s, c, o, e, instrument, conn=conn)
            )
    return all_measurements


def organize_measurements_by_chamber(all_measurements):
    """Organize measurements by chamber ID."""
    cycle_dict = {}
    for mes in all_measurements:
        cycle_dict.setdefault(mes.id, []).append(mes)
    return cycle_dict


### Trigger and State Handling Functions ###


def handle_triggers(args, all_chambers, graph_names):
    """
    Handle Dash triggers based on user interaction and return necessary information.
    """
    (
        attr_relays,
        pt_clicks,
        gas_relays,
        date_range,
        stored_settings,
        buttons,
        prev_clicks,
        next_clicks,
        skip_invalid,
        skip_valid,
        selected_chambers,
        index,
        chamber,
        parse_range,
        selected_instrument,
    ) = args
    if selected_instrument is None:
        return None, None, None, None, None, None
    logger.info(selected_instrument)
    start_date = pd.to_datetime(date_range.get("start_date"))
    end_date = pd.to_datetime(date_range.get("end_date"))
    logger.debug(start_date)
    logger.debug(end_date)
    date_range = (start_date, end_date)
    logger.info("Running.")
    logger.info(f"Triggered key: {ctx.triggered_id}")
    logger.debug(f"Start date: {start_date}.")
    selected_chambers = selected_chambers or all_chambers

    skips = None
    if skip_invalid:
        skips = True
    if skip_valid:
        skips = False
    if skip_invalid and skip_valid:
        skips = None

    if isinstance(ctx.triggered_id, dict):
        triggered_elem = ctx.triggered_id["index"]
    else:
        triggered_elem = ctx.triggered_id if ctx.triggered else None

    selected_instrument = json.loads(selected_instrument)
    serial = selected_instrument["serial"]

    if triggered_elem == "parse-range" or triggered_elem == "used-instrument-select":
        point_data = flux_range_to_df(
            start_date, end_date, selected_chambers, skips, serial
        )
        if point_data.empty:
            return None, None, point_data, None, None, None

        index = pd.to_datetime(point_data["start_time"].iloc[0])
        if len(point_data) == 0 and point_data.empty:
            return None, None, point_data, None, None, None

    logger.debug(selected_chambers)
    logger.debug(all_chambers)

    df_meas = flux_range_to_df(start_date, end_date, selected_chambers, skips)
    if len(df_meas) == 0 and df_meas.empty:
        return None, None, df_meas, None, None, None

    if (
        triggered_elem == "chamber-select"
        or triggered_elem == "skip-invalid"
        or triggered_elem == "skip-valid"
        or triggered_elem == "toggle-valid"
    ):
        nearest_index = (df_meas["start_time"] - pd.to_datetime(index)).abs().idxmin()
        index = df_meas.loc[nearest_index].get("start_time")

    if df_meas.empty:
        return None, None, df_meas, None, None, None
    if index == 0:
        index = df_meas["start_time"].iloc[0]

    logger.debug(df_meas["chamber_id"].unique())

    measurements = df_meas.copy()

    picked_point = None
    logger.debug(f"Skip invalid value: {skip_invalid}")

    if triggered_elem == "next-button":
        logger.debug("prev-button clicked.")
        index = increment_index(index, df_meas)

    if triggered_elem == "prev-button":
        logger.debug("next-button clicked.")
        index = decrement_index(index, df_meas)

    attr_plots = graph_names[1]

    # pt_clicks length is always the same as attr_plots so we can enumerate for
    # comparisons
    if triggered_elem in attr_plots and pt_clicks:
        for i, attr_plot in enumerate(attr_plots):
            logger.debug(attr_plot)
            if triggered_elem == attr_plot:
                pt = pt_clicks[i]
        logger.debug("attribute-graph item clicked.")
        logger.debug(pt)
        picked_point = pd.to_datetime(pt.get("points")[0].get("x"), utc=True)

    if picked_point is not None:
        logger.debug("Graph point selected.")
        logger.debug(picked_point)
        measurement = df_meas.loc[df_meas["start_time"] == picked_point].iloc[0]
        index = picked_point
        logger.info(f"index: {index}")
    else:
        measurement = df_meas.loc[df_meas["start_time"] == index].iloc[0]

    # parse gas relaouts to see if lagtimes or flux calculation areas were
    # changed.
    gas_plots = graph_names[0]
    logger.debug(triggered_elem)

    instrument = measurement.get("instrument_model").replace("-", "")
    serial = measurement.get("instrument_serial")
    logger.debug(measurement)
    m = MeasurementCycle(
        measurement.get("chamber_id"),
        measurement.get("start_time"),
        measurement.get("close_offset"),
        measurement.get("open_offset"),
        measurement.get("end_offset"),
        instruments.get(instrument)(serial),
    )
    if triggered_elem in gas_plots:
        for i, gas_plot in enumerate(gas_plots):
            if gas_relays[i] is None:
                continue
            if triggered_elem == gas_plot:
                gas = gas_plot.split("-")[0]
                parse_relayout(gas_relays[i], m, gas)
                measurements = update_row(m, measurements)
                logger.debug(measurements.iloc[0].get("lagtime"))
    measurement = m

    return (
        triggered_elem,
        index,
        measurements,
        measurement,
        selected_chambers,
        date_range,
    )


def update_row(measurement, measurements):
    df1 = measurements
    df2 = measurement.get_attribute_df()
    key = "start_time"
    columns_to_update = list(df2.columns)
    index_to_replace = df1[df1[key] == df2.iloc[0][key]].index
    for col in columns_to_update:
        if col in df2.columns:
            df1.loc[index_to_replace, col] = df2.iloc[0][col]
    return df1


def no_data_response(selected_chambers, graph_names):
    """Return an empty response if no data is available."""
    layout = Layout(
        annotations=[
            dict(
                name="draft watermark",
                text="No calculated data in timerange",
                textangle=0,
                opacity=0.4,
                font=dict(color="black", size=40),
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
            )
        ]
    )
    empty_figures = [[Figure(layout=layout) for _ in names] for names in graph_names]

    return (
        *empty_figures,
        "No data available",
        no_update,
        selected_chambers,
        # points_store,
    )


def decrement_index(index, measurements):
    """Decrement the current index with wrap-around."""
    # Filter rows with datetime greater than target_datetime
    filtered_df = measurements[measurements["start_time"] < index]
    if not filtered_df.empty:
        # Get the row with the smallest datetime greater than the current index
        next_row_index = filtered_df["start_time"].idxmax()
        next_datetime = filtered_df.loc[next_row_index, "start_time"]
    else:
        # Wrap around: get the smallest datetime in the DataFrame
        next_datetime = measurements["start_time"].max()

    index = next_datetime
    return index


def increment_index(index, measurements):
    """Increment the current index with wrap-around."""
    # Filter rows with datetime greater than target_datetime
    filtered_df = measurements[measurements["start_time"] > index]
    if not filtered_df.empty:
        # Get the row with the smallest datetime greater than the current index
        next_row_index = filtered_df["start_time"].idxmin()
        next_datetime = filtered_df.loc[next_row_index, "start_time"]
    else:
        # Wrap around: get the smallest datetime in the DataFrame
        next_datetime = measurements["start_time"].min()

    index = next_datetime
    return index


def load_measurement_data(measurement):
    """Load data for a specific measurement from InfluxDB."""
    instrument = measurement.get("instrument_model").replace("-", "")
    serial = measurement.get("instrument_serial")
    m = MeasurementCycle(
        measurement.get("chamber_id"),
        measurement.get("start_time"),
        measurement.get("close_offset"),
        measurement.get("open_offset"),
        measurement.get("end_offset"),
        instruments.get(instrument)(serial),
    )

    return m


def execute_actions(triggered_id, measurement, measurements, date_range):
    """Execute specific actions based on user input (e.g., find max, delete lag, push data)."""
    # if triggered_id == "extend-time":
    #     logger.debug("extend-time clicked.")
    #     measurement.end_extension = 2
    #     measurement.data = None
    #     measurement.get_data(ifdb_read_dict)
    if triggered_id == "del-lagtime":
        logger.debug("del-lagtime clicked.")
        measurement.del_lagtime()
    if triggered_id == "max-r":
        logger.debug("max-r clicked.")
        measurement.lagtime = 0
        measurement.get_max()
        logger.info(measurement.attribute_dict)
        push_single_point(measurement)
    if triggered_id == "push-all":
        logger.debug("push-all clicked.")
    if triggered_id == "push-single":
        logger.debug("push-single clicked.")
        logger.debug(measurement.attribute_dict)
        logger.debug(measurement.air_temperature)
        # measurement.get_max()
        push_single_point(measurement)

    if triggered_id == "mark-invalid":
        logger.debug("mark-invalid clicked.")
        measurement.is_valid_manual = False
        logger.debug(measurement.attribute_df.to_dict())
        push_single_point(measurement)

    if triggered_id == "toggle-valid":
        logger.debug("toggle-valid clicked.")
        logger.info(measurement.is_valid)
        logger.info(measurement.is_valid_manual)
        if measurement.is_valid is True:
            measurement.is_valid_manual = False
        else:
            measurement.is_valid_manual = True
        logger.info(measurement.is_valid)
        logger.info(measurement.is_valid_manual)
        push_single_point(measurement)
    if triggered_id == "reset-cycle":
        logger.debug("reset-cycle clicked.")
        measurement.lagtimes_s = 0
    if triggered_id == "add-time":
        measurement.end_offset = measurement._end_offset + 120
        measurement.data = gas_table_to_df(measurement.start_time, measurement.end)
        push_single_point(measurement)
    if triggered_id == "substract-time":
        measurement.end_offset = measurement._end_offset - 120
        measurement.data = gas_table_to_df(measurement.start_time, measurement.end)
        push_single_point(measurement)


### Plotting Functions ###


def create_gas_plots(measurement, graph_names, settings):
    """Create CH4 and CO2 plots using Plotly."""
    figs = [Figure() for _ in graph_names]
    gases = [name.split("-")[0] for name in graph_names]
    logger.debug(gases)
    colors = ["blue", "green", "red"]
    if measurement.data is not None and not measurement.data.empty:
        figs = [
            measurement.mk_gas_plot(
                gas, colors[i], zoom_to_calc=settings["zoom_to_calc"]["value"]
            )
            for i, gas in enumerate(gases)
        ]
    return figs


def create_attribute_graph(
    measurement,
    measurements,
    selected_chambers,
    index,
    triggered_elem,
    date_range,
    gas_graphs,
    attribute,
    gas=None,
):
    """
    Create the lag graph with optional highlighting and zooming based on triggered actions.
    """
    measurements = update_row(measurement, measurements)
    attribute_plot = mk_attribute_plot(
        measurement,
        measurements,
        selected_chambers,
        index,
        date_range,
        attribute,
        gas,
    )
    return attribute_plot


def push_single_point(measurement):
    """Push a single measurement's lag data to InfluxDB."""
    m = measurement
    single_flux_to_table(m.attribute_df)


def generate_measurement_info(measurement, index, measurements):
    """Generate text-based information about the current measurement."""
    valid_str = "Valid: True" if measurement.is_valid else "Valid: False"
    has_errors_str = (
        "Has errors: True" if measurement.has_errors else "Has errors: False"
    )
    ch4_r = measurement.r.get("CH4")
    return f"Measurement {index}/{len(measurements)} - Date: {measurement.start_time.date()} {valid_str} {has_errors_str}  {measurement.r_std} {round(ch4_r)} {measurement._error_string}"


def parse_relayout(relayout, measurement, gas):
    """
    Function that checks the shapes in the gas plots to determine if they have
    been moved and updates the measurements if they have.
    """
    logger.debug("Parsing relayout")
    # TODO: moving the lagtime bar should find the max +-3 seconds around the
    # bar to accurately find the lagtime.

    # dictionary length should be =< 2 if shapes were not clicked
    if len(relayout) > 2:
        keys = []
        for key in relayout:
            keys.append(key)
            # hack fix when user clicks Reset axes on top right of plot
            try:
                shape_index = int(key.split("[")[1].split("]")[0])
            except Exception as e:
                logger.debug(e)
                return
        if "shapes" not in ",".join(keys):
            return
        if shape_index == 0:
            if len(dict(relayout)) > 1:
                logger.debug("New manual lagtime set.")
                manual_lag = pd.to_datetime(dict(relayout).get("shapes[0].x0"))
                manual_lag = manual_lag.tz_localize("UTC", ambiguous=True)
                lag_s = int((manual_lag - measurement.og_open).total_seconds())
                # measurement.lagtime = lag_s
                measurement.manual_lag = True
                measurement.get_max(manual_lag=lag_s)
                single_flux_to_table(measurement.attribute_df)
        if shape_index == 1:
            logger.debug("New area for flux calculation set.")

            # limit s&e to close and open time in case they go over
            s = pd.to_datetime(dict(relayout).get("shapes[1].x0"))
            s = s.tz_localize("UTC", ambiguous=True)
            s = measurement.close if s < measurement.close else s

            e = pd.to_datetime(dict(relayout).get("shapes[1].x1"))
            e = e.tz_localize("UTC", ambiguous=True)
            e = measurement.open if e > measurement.open else e

            logger.debug(s)
            logger.debug(e)
            s_offset = int((s - measurement.start_time).total_seconds())
            e_offset = int((e - measurement.start_time).total_seconds())
            measurement.calc_offset_s[gas] = s_offset
            measurement.calc_offset_e[gas] = e_offset
            measurement.calculate_flux(gas)
            single_flux_to_table(measurement.attribute_df)


def generate_key(row):
    # Combine the relevant columns into a string
    row_data = f"{row['datetime']}-{row['CH4']}-{row['CO2']}"
    # Generate a hash (using MD5 here for simplicity; SHA-256 is also an option)
    return hashlib.md5(row_data.encode("utf-8")).hexdigest()


def generate_cycles(start, end, chambers, skip_invalid, skip_valid):
    """Generate cycles from Cycle_tbl in the db."""
    df = cycle_table_to_df(start, end)
    cycles = []

    df["START"] = df["start_time"]
    df["CHAMBER"] = df["chamber_id"]
    df["CLOSE"] = df["START"] + pd.to_timedelta(df["close_offset"], unit="s")
    df["OPEN"] = df["START"] + pd.to_timedelta(df["open_offset"], unit="s")
    df["END"] = df["START"] + pd.to_timedelta(df["end_offset"], unit="s")
    df = df[["CHAMBER", "START", "CLOSE", "OPEN", "END"]]
    # for _, row in df.iterrows():
    #     chamber = row["chamber_id"]
    #     if chamber not in chambers:
    #         continue
    #
    #     cycle = {}
    #     start = row["start_time"]
    #     cycle["CHAMBER"] = row["chamber_id"]
    #     cycle["START"] = row["start_time"]
    #     cycle["CLOSE"] = start + pd.Timedelta(seconds=row["close_seconds"])
    #     cycle["OPEN"] = start + pd.Timedelta(seconds=row["open_seconds"])
    #     cycle["END"] = start + pd.Timedelta(seconds=row["end_seconds"])
    #     cycles.append(cycle)
    return df.to_dict(orient="records")


def parse_date_range(start, end):
    if len(start) > 10:
        start_date = pd.to_datetime(start, format="ISO8601")
    else:
        start_date = pd.to_datetime(start, format="%Y-%m-%d")
        start_date = start_date.tz_localize(
            "Europe/Helsinki", ambiguous=True
        ).tz_convert("UTC")

    if len(end) > 10:
        end_date = pd.to_datetime(end, format="ISO8601")
    else:
        end_date = pd.to_datetime(end, format="%Y-%m-%d")
        end_date = end_date.tz_localize("Europe/Helsinki", ambiguous=True).tz_convert(
            "UTC"
        )
    date_range = (start_date, end_date)
    logger.debug(date_range)
    return date_range


def parse_cycle_to_db(start, end, ifdb_dict, chamber_map):
    """
    Parse the raw chamber protocol from influxdb to a new format
    """
    meas_dict = {"bucket": "Testi", "measurement": "AC_STATE", "fields": None}
    with init_client(ifdb_dict) as client:
        df = just_read(ifdb_dict, meas_dict, client, start_ts=start, stop_ts=end)
        df.sort_values(by="id").reset_index(drop=True, inplace=True)
        # NOTE: needs to be done in this seemingly overcomplicated way since
        # each cycle’s start time is the same (almost always) as end time of the previous cycle
        # so only grouping by datetime will cause issues since there's no
        # telling which order they'll end up

        df["state"] = df["state"].astype(int)
        # telegraf pushes chamber ids with spaces
        df["id"] = df["id"].str.strip().astype(int)
        df["id"] = df["id"].map(chamber_map)
        # create list of dataframes where each dataframe only has cycles from
        # one chamber
        id_groups = [group for _, group in df.groupby("id")]
        grouped = []
        for id_group in id_groups:
            # calculate time differences between rows, and when time difference
            # is over 16minutes, increment. (Realistically its always 3 hours
            # with the oulanka cycle) This creates a unique identifier for
            # each cycle
            id_group["cycle"] = (
                id_group["datetime"].diff() > pd.Timedelta(minutes=16)
            ).cumsum()
            # group each cycle into its own dataframe and append to list
            [grouped.append(group) for _, group in id_group.groupby("cycle")]
    # sort the list of dataframes by start time
    sorts = sorted(grouped, key=lambda df: df["datetime"].iloc[0])
    # concatenate all dataframes a single dataframe

    dfs = []
    for df in sorts:
        if df.empty:
            continue
        # needs open, close and deactivate
        if df["state"].nunique() != 3:
            continue
        chamber = df["id"].iloc[0]
        # first 10 is the start
        start = df.loc[df["state"] == 10, "datetime"].iloc[0]
        # first 11 is the close
        close = df.loc[df["state"] == 11, "datetime"].iloc[0]
        # second 10 should be the opening, use -1 to grab the last to be
        # sure
        open = df.loc[df["state"] == 10, "datetime"].iloc[-1]
        # last 00 is the end
        end = df.loc[df["state"] == 0, "datetime"].iloc[-1]
        data = {
            "chamber_id": [chamber],
            "start_time": [start],
            "close_offset": [int((close - start).total_seconds())],
            "open_offset": [int((open - start).total_seconds())],
            "end_offset": [int((end - start).total_seconds())],
        }
        group = pd.DataFrame(data)
        # if any of these are 0 or less there's something wrong
        if np.any(group[["close_offset", "open_offset", "end_offset"]].to_numpy() < 0):
            continue
        dfs.append(group)

    dfa = pd.concat(dfs)

    return dfa
