import json
import io
import base64
import logging
import traceback
import pandas as pd

from .db import engine
from .measuring import instruments
from .data_mgt import (
    df_to_gas_table,
    df_to_cycle_table,
    df_to_meteo_table,
    df_to_volume_table,
    cycle_table_to_df,
    flux_table_to_df,
    check_existing_instrument,
    add_instrument,
)
from .utils import (
    process_measurement_file,
    process_measurement_zip,
    process_protocol_file,
    process_protocol_zip,
    init_from_cycle_table,
)

logger = logging.getLogger("defaultLogger")


def read_gas_init_input(use_class, serial, model, name, contents, filename):
    """Read data passed from the settings page"""
    if serial is None or model is None:
        return "Select instrument or fill in instrument details", ""
    if not check_existing_instrument(serial, model, use_class):
        add_instrument(model, serial, use_class, name)

    content_type, content_str = contents.split(",")
    ext = filename.split(".")[-1].lower()
    decoded = base64.b64decode(content_str)
    instrument = instruments.get(use_class)
    if instrument is None:
        return f"Incorrect instrument: {serial} {model}"

    instrument = instrument(serial)
    file_exts = ["csv", "data", "dat"]
    try:
        if ext in file_exts:
            df = process_measurement_file(
                io.StringIO(decoded.decode("utf-8")), instrument
            )
            in_rows = len(df)
            df["instrument_serial"] = instrument.serial
            df["instrument_model"] = instrument.model
            df["datetime"] = (
                df["datetime"]
                .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                .dt.tz_convert("UTC")
            )
            pushed_data, dupes = df_to_gas_table(df)
            push_rows = len(pushed_data)
            return "", f"Pushed {push_rows}/{in_rows}"

        if ext == "zip":
            print("process zip")
            push_rows, in_rows = process_measurement_zip(
                io.BytesIO(decoded), instrument
            )
            return "", f"Pushed {push_rows}/{in_rows} rows."
        else:
            return "Wrong filetype extension", ""
    except Exception as e:
        return f"Exception {e}", ""


def read_cycle_init_input(contents, filename, chamber_map):
    """Read data passed from the settings page"""
    # global instruments
    content_type, content_str = contents.split(",")
    decoded = base64.b64decode(content_str)
    try:
        if "csv" in filename:
            df = pd.read_csv(io.StringIO(decoded.decode("utf-8")))
            df["start_time"] = pd.to_datetime(df["start_time"], format="ISO8601")
            try:
                df["start_time"] = (
                    df["start_time"]
                    .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                    .dt.tz_convert("UTC")
                )
            except Exception:
                pass
            inrows = len(df)
            pushed_data = df_to_cycle_table(df)

            if pushed_data.empty:
                row_count = 0
            else:
                row_count = len(pushed_data)
            return "", f"Pushed {inrows}/{row_count}"

        # Read the CSV into a Pandas DataFrame
        if "log" in filename:
            df = process_protocol_file(
                io.StringIO(decoded.decode("utf-8")), chamber_map
            )
            in_cycles = len(df)

            pushed_data = df_to_cycle_table(df)
            if pushed_data.empty:
                row_count = 0
            else:
                row_count = len(pushed_data)
            return "", f"Pushed {row_count}/{in_cycles}"

        if "zip" in filename:
            df = process_protocol_zip(io.BytesIO(decoded), chamber_map)
            in_cycles = len(df)
            pushed_data = df_to_cycle_table(df)
            if pushed_data.empty:
                row_count = 0
            else:
                row_count = len(pushed_data)
            return "", f"Pushed {row_count}/{in_cycles}"
    except Exception as e:
        return f"Returned exception {e}", ""


def read_meteo_init_input(source, contents, filename):
    """Read data passed from the settings page"""
    # global instruments
    if source is None:
        return "Select site.", ""
    content_type, content_str = contents.split(",")
    ext = filename.split(".")[-1].lower()
    decoded = base64.b64decode(content_str)
    file_exts = ["csv", "data", "dat"]

    def read_meteo_file(file):
        df = pd.read_csv(file)
        df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")
        df["datetime"] = (
            df["datetime"]
            .dt.tz_localize(
                "Europe/Helsinki", ambiguous=True, nonexistent="shift_forward"
            )
            .dt.tz_convert("UTC")
        )
        return df

    try:
        file_exts = ["csv"]
        if ext in file_exts:
            logger.debug("Read file.")
            df = read_meteo_file(io.StringIO(decoded.decode("utf-8")))
            logger.debug("Adding source.")
            df["source"] = source
            in_rows = len(df)
            logger.debug("Pushing to table")
            pushed_data = df_to_meteo_table(df)
            push_rows = len(pushed_data)
            return "", f"Pushed {push_rows}/{in_rows}"

        else:
            return "Wrong filetype extension", ""
    except Exception as e:
        logger.debug(traceback.format_exc())
        return f"Exception {e}", ""


def init_flux(init, start, end, instrument, meteo):
    # if instrument is None:
    #     return "Select used instrument"
    # if meteo is None:
    #     return "Select meteo source"
    # if start is None:
    #     return "Give start date"
    # if end is None:
    #     return "Give end date"
    # try:
    #     pd.to_datetime(start, format="%Y-%m-%d %H:%M", errors="raise")
    #     pd.to_datetime(end, format="%Y-%m-%d %H:%M", errors="raise")
    # except Exception as e:
    #     return f"{e}"
    #     # return "give YYYY-MM-DD date"
    logger.info("Initiating")
    fluxes = flux_table_to_df()
    dupes = set(fluxes["start_time"])
    logger.debug(fluxes)
    with engine.connect() as conn:
        df = cycle_table_to_df(start, end, conn)
        if df.empty or df is None:
            return f"No cycles between {start} and {end}."
        logger.debug(df)
        df = df[~df["start_time"].isin(dupes)]
        df.sort_values("start_time", inplace=True)
        logger.debug(df)
        instrument = json.loads(instrument)
        serial = instrument["serial"]
        use_class = instrument["python_class"]
        instrument = instruments.get(use_class)(serial)
        init_from_cycle_table(df, serial=serial, use_class=use_class, conn=conn)


def read_volume_init_input(contents, filename):
    """Read data passed from the settings page"""
    # global instruments
    content_type, content_str = contents.split(",")
    ext = filename.split(".")[-1].lower()
    decoded = base64.b64decode(content_str)
    file_exts = ["csv", "data", "dat"]

    def read_volume_file(file):
        df = pd.read_csv(file)
        df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")
        df["datetime"] = (
            df["datetime"]
            .dt.tz_localize(
                "Europe/Helsinki", ambiguous=True, nonexistent="shift_forward"
            )
            .dt.tz_convert("UTC")
        )
        df["chamber_id"] = df["chamber_id"].astype(str)
        df["chamber_height"] = df["chamber_height"].astype(float)
        return df

    try:
        file_exts = ["csv"]
        if ext in file_exts:
            logger.debug("Read file.")
            df = read_volume_file(io.StringIO(decoded.decode("utf-8")))
            in_rows = len(df)
            logger.debug("Pushing to table")
            pushed_data = df_to_volume_table(df)
            push_rows = len(pushed_data)
            return "", f"Pushed {push_rows}/{in_rows}"

        else:
            return "Wrong filetype extension", ""
    except Exception as e:
        logger.debug(traceback.format_exc())
        return f"Exception {e}", ""
