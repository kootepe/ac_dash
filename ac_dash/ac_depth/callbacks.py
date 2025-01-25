import io
import json
import base64
from datetime import datetime as dt


import logging
from dash import Output, Input, State, ctx, ALL, html, dcc, dash_table
from datetime import datetime as dt
from flask_login import current_user

from sqlalchemy.exc import IntegrityError
from ..common_utils.influxdb_helper import (
    ifdb_push,
    delete_by_uuid,
)
from ..common_utils.utils import (
    LOCAL_TZ,
    CONTAINER_TZ,
)
from .utils import (
    parse_local_datetime,
    load_config,
    IfdbDepthPoint,
    query_log_point,
    show_old_measurements,
)
from ..data_mgt import df_to_volume_table, del_volume_measurement
import pandas as pd

logger = logging.getLogger("defaultLogger")


# ifdb_dict = load_config().get("ac_depth_push")

height_modifier = {
    "mm": 0.001,
    "cm": 0.01,
    "dm": 0.1,
    "m": 1,
}

expected = ["nw", "sw", "ne", "se", "mid", "chamber_height"]


def parse_contents(contents, filename, time, measurement_cols, unit, has_snow):
    logger.debug("Uploaded measurements")
    content_type, content_string = contents.split(",")
    measurement_cols = [col.lower() for col in measurement_cols]

    if has_snow is None:
        has_snow = 0
    else:
        if len(has_snow) > 0:
            has_snow = 1
        else:
            has_snow = 0
    decoded = base64.b64decode(content_string)
    if "csv" in filename or "txt" in filename:
        df = pd.read_csv(
            io.StringIO(decoded.decode("utf-8")), names=["measurement_id", "value"]
        )
        file_date = dt.strptime(f"{filename[:10]} 12:00", "%Y_%m_%d %H:%M")
        # split first column
        df[["chamber_id", "measurement"]] = df["measurement_id"].str.split(
            "_", n=2, expand=True
        )
        # keeps these
        df = df[["chamber_id", "measurement", "value"]]
        df["measurement"] = df["measurement"].str.lower()

        # group by chamber id
        dfs = []
        for gr, df in df.groupby("chamber_id"):
            df = df.pivot_table(
                index="chamber_id",
                columns="measurement",
                values="value",
                fill_value=-9999,
            ).reset_index()

            dfs.append(df)
        dfa = pd.concat(dfs)
        # sort by chamber_id
        dfa = dfa.sort_values(by="chamber_id", key=lambda col: col.astype(int))
        dfa["datetime"] = file_date
        dfa["has_snow"] = has_snow
        dfa["unit"] = unit
        dfa.fillna(-9999, inplace=True)

        dfa["chamber_height"] = (
            (dfa[["nw", "sw", "ne", "se"]].sum(axis=1) / 4) + dfa["mid"]
        ) / 2
        # reorder columns
        matching_columns = [col for col in measurement_cols if col in dfa.columns]
        extra_columns = [col for col in dfa.columns if col not in measurement_cols]
        dfa = dfa[extra_columns + matching_columns]
    else:
        return html.Div(
            ["Unsupported file format. Please upload a comma separated CSV file."]
        )

        # )
    data = dfa.to_dict("records")
    columns = [{"name": col, "id": col} for col in dfa.columns]
    return data, columns


def register_callbacks(app, chambers, in_measurements):
    # flatten list
    chambers = [item for row in chambers for item in row]

    @app.callback(
        Output("dl-template", "data"),
        Input("btn-template", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_template(n_clicks):
        # Convert DataFrame to CSV
        date = dt.today().strftime("%Y_%m_%d")
        csv = ""
        for ch in chambers:
            root = f"{ch}_"
            for m in in_measurements:
                csv += f"{root}{m},\n"

        csv_buffer = io.StringIO(csv)
        csv_buffer.seek(0)
        # Return the CSV data as a downloadable file
        return dcc.send_bytes(
            csv_buffer.getvalue().encode(),
            filename=f"{date}_volume_measurement_template.txt",
        )

    @app.callback(
        Output("date-input", "value"),
        Input("dummy-div", "n_clicks"),
    )
    def change_date(n_clicks):
        date = dt.today().strftime("%Y-%m-%d %H:%M")
        return date

    @app.callback(
        Output("all-upload-warning-div", "children"),
        Output("all-upload-text-div", "children"),
        Input("upload-all", "n_clicks"),
        State("has-snow", "value"),
        State("data-table", "data"),
        State("measurement-unit", "value"),
        prevent_initial_call=True,
    )
    def upload_all(n_clicks, has_snow, data, unit):
        if ctx.triggered_id != "upload-all":
            return ""
        if len(data) == 0:
            return "No data."

        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["datetime"] = (
            df["datetime"]
            .dt.tz_localize("Europe/Helsinki", ambiguous=True)
            .dt.tz_convert("UTC")
        )

        in_len = len(df)
        # convert values to meters
        for col in expected:
            if col in df.columns:
                df[col] = df[col] * height_modifier.get(unit)
            else:
                df[col] = -9999
        # lazy sanity check
        if n_clicks < 2:
            if (df[expected] > 1).any().any():
                return (
                    "Measurements over 1 meter, for real? Click again to upload anyway.",
                    "",
                )
            if (df[expected] < 0.1).any().any():
                return (
                    "Measurements under 10 cm, for real? Click again to upload anyway.",
                    "",
                )

        try:
            df["unit"] = "m"
            pushed_df = df_to_volume_table(df)
            plen = len(pushed_df)
            if plen == 0:
                return f"Pushed {len(pushed_df)}/{in_len} rows.", ""
            else:
                return "", f"Pushed {len(pushed_df)}/{in_len} rows."
        except (IntegrityError, ValueError) as e:
            return f"{e}", ""

        for p in data:
            pt = IfdbDepthPoint(
                p.get("datetime"),
                p.get("chamber_id"),
                *[p.get(meas.lower()) for meas in in_measurements],
                has_snow,
                unit,
            ).to_dict()
            # ifdb_push(pt, ifdb_dict)
        return "", "Upload successful."

    @app.callback(
        Output("csv-warning-div", "children"),
        Output("data-table", "data"),
        Output("data-table", "columns"),
        Output("upload-display", "style"),
        State("date-input", "value"),
        State("measurement-unit", "value"),
        State("has-snow", "value"),
        Input("up-template", "contents"),
        State("up-template", "filename"),
        prevent_initial_call=True,
    )
    def upload_csv(datetime, unit, has_snow, contents, filename):
        logger.debug(ctx.triggered_id)
        # time = datetime
        time = parse_local_datetime(datetime)
        # time = dt.strptime(datetime, "%Y-%m-%d %H:%M")
        # time = LOCAL_TZ.localize(time).astimezone(CONTAINER_TZ)
        data, cols = parse_contents(
            contents, filename, time, in_measurements, unit, has_snow
        )
        style = {"width": "fit-content", "margin": "20px"}
        return "", data, cols, style

    @app.callback(
        Output("text-div", "children"),
        Output("warning-div", "children"),
        Input({"type": "submit", "index": ALL}, "n_clicks"),
        State("date-input", "value"),
        State("has-snow", "value"),
        State("measurement-unit", "value"),
        Input(
            {
                "type": "del-button",
                "id": ALL,
                "index": ALL,
                "time": ALL,
                "chamber_id": ALL,
            },
            "n_clicks",
        ),
        *[
            State({"type": f"{meas.lower()}-submit", "index": ALL}, "value")
            for meas in in_measurements
        ],
    )
    def display_chamber(*args):
        submit_clicks = args[0]
        datetime = args[1]
        # TODO: store has_snow value in cookies
        has_snow = args[2]
        unit = args[3]
        del_button = args[4]
        measurement_inputs = args[len(in_measurements) :]

        measurement_names = [measurement.lower() for measurement in in_measurements]
        role = current_user.role
        # measurement = ifdb_dict.get("measurement")
        # pts = query_log_point(measurement, ifdb_dict)
        # old_pts = show_old_measurements(pts, role)
        old_pts = ""
        # if not old_pts:
        #     old_pts = ""

        if unit not in height_modifier.keys():
            return old_pts, "Wrong unit for height, use mm, cm, dm, or m"

        try:
            dt.strptime(datetime, "%Y-%m-%d %H:%M")
        except Exception:
            return old_pts, "Wrong time format, use 'YYYY-MM-DD HH:MM'"

        if ctx.triggered_id is None:
            logger.debug("None triggered")
            return old_pts, ""

        if isinstance(ctx.triggered_id, str):
            return old_pts, ""

        if ctx.triggered_id == "del-button":
            logger.debug("Delete pressed")

        elem_type = ctx.triggered_id.get("type")
        if elem_type == "del-button":
            logger.debug("Deleting point")
            time = dt.strptime(ctx.triggered_id.get("time"), "%Y-%m-%d %H:%M")
            # delete_by_uuid(ifdb_dict, time, ctx.triggered_id.get("id"))
            del_volume_measurement(
                # time.astimezone(CONTAINER_TZ),
                time,
                ctx.triggered_id.get("chamber_id"),
            )
            # pts = query_log_point(measurement, ifdb_dict)
            pts = pd.DataFrame()
            old_pts = show_old_measurements(pts, role)
            return old_pts, ""
        if elem_type == "submit":
            index = ctx.triggered_id.get("index")

            # list of lists with items from the inputs
            data = list(zip(*measurement_inputs))
            print(measurement_inputs)
            # create dict with chamber numbers as keys
            data_dict = dict(zip(chambers, data))

            if elem_type == "submit":
                values = data_dict.get(index)

                if None in values:
                    return (
                        old_pts,
                        "All 5 inputs must have numeric values. Use -9999 for no value.",
                    )
                if len(has_snow) > 0:
                    has_snow = 1
                else:
                    has_snow = 0
                values = [val * height_modifier.get(unit) for val in values]
                to_db = dict(zip(measurement_names, values))
                # the indexes of the elements are the same as the chamber ids
                pt = IfdbDepthPoint(
                    parse_local_datetime(datetime),
                    index,
                    *[to_db.get(meas) for meas in measurement_names],
                    has_snow,
                    unit,
                ).to_dict()
                time = parse_local_datetime(datetime)
                logger.debug(values)

                if None not in values or -9999 not in values:
                    chamber_height = ((sum(values[:-1]) / 4) + values[-1]) / 2
                else:
                    chamber_height = -9999

                data = {
                    "chamber_id": [index],
                    "datetime": [time],
                    **{meas: to_db.get(meas) for meas in measurement_names},
                    "chamber_height": [chamber_height],
                    "height_unit": ["m"],
                }
                df = pd.DataFrame(data)

                try:
                    pass
                    df_to_volume_table(df)
                except (IntegrityError, ValueError):
                    return old_pts, "DATAPOINT EXISTS IN LOCAL DB"

                # ifdb_push(pt, ifdb_dict)
                print("final old")
                # pts = query_log_point(measurement, ifdb_dict)
                pts = pd.DataFrame()
                old_pts = show_old_measurements(pts, role)

                return old_pts, ""


def height_sanity_check(heights, unit):
    pass
