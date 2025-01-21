import os
import pandas as pd
import csv
import logging
from flask import redirect, request, jsonify, session, url_for, Response, Blueprint
from flask_restful import Resource
from flask_login import login_required, login_user
from ..server import User, login_manager, server
from werkzeug.security import check_password_hash
from ..data_mgt import (
    cycle_table_to_df,
    flux_table_to_df,
    df_to_cycle_table,
    df_to_gas_table,
    df_to_meteo_table,
)

from ..utils import (
    process_protocol_file,
    process_protocol_zip,
    process_measurement_file,
    process_measurement_zip,
    load_config,
    init_from_cycle_table,
)

from ..measuring import instruments
from ..db import engine

logger = logging.getLogger("defaultLogger")

(_, _, _, chamber_map, _) = load_config()

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


def register_api(api):
    api.add_resource(FluxApi, "/api/fluxes")
    api.add_resource(CycleApi, "/api/cycle_api")
    api.add_resource(GasApi, "/api/gas_api")
    api.add_resource(MeteoApi, "/api/meteo_api")
    api.add_resource(InitFluxApi, "/api/init_api")


@auth_bp.route("/login/", methods=["POST"])
def login_route():
    data = request.json
    username = data.get("username")
    password = data.get("password")

    user = User.query.filter_by(username=username).first()
    if user is None:
        return jsonify({"error": "Invalid credentials", "success": False}), 401

    if check_password_hash(user.password, password):
        login_user(user)
        session["logged_in"] = True
        return jsonify({"message": "Logged in successfully", "success": True}), 200
    else:
        return jsonify({"error": "Invalid credentials", "success": False}), 401
    return redirect("/login/")


class FluxApi(Resource):
    @login_required
    def get(self):
        csv = flux_table_to_df().to_csv(index=False)
        response = Response(
            csv,
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename=data.csv"},
        )
        return response

    def post(self):
        pass


def convert_datetime_to_str(df):
    """
    Convert all columns with dtype datetime in a pandas DataFrame to string type.

    Parameters:
    df (pd.DataFrame): The input pandas DataFrame.

    Returns:
    pd.DataFrame: The DataFrame with datetime columns converted to string.
    """
    for col in df.select_dtypes(include=["datetime", "datetime64[ns]"]).columns:
        df[col] = df[col].astype(str)
        return df


class GasApi(Resource):
    def get(self):
        pass

    @login_required
    def post(self):
        """
        Upload a CSV file and process it.
        """
        # Check if a file is provided
        if "file" not in request.files:
            return {"message": "No file provided"}, 400

        file = request.files["file"]
        # check for zero lengt files
        file_length = file.seek(0, os.SEEK_END)
        file.seek(0, os.SEEK_SET)
        if file_length < 10:
            return {"message": f"No data in file {file.filename}"}, 400

        instrument = request.form.get("instrument", None)
        serial = request.form.get("serial", None)
        # try and read the first two rows of the file for model and serial
        if not instrument:
            file.stream.seek(0)
            reader = csv.reader(file.stream.read().decode("utf-8").splitlines())
            rows = {}
            for i, row in enumerate(reader):
                row_data = row[0].split("\t")
                rows[row_data[0]] = row_data[1]
                if i == 1:
                    break
            instrument = rows.get("Model:").replace("-", "")
            serial = rows.get("SN:")
            file.stream.seek(0)

        # Check if the file has a valid filename
        if file.filename == "":
            return {"message": "No selected file"}, 400

        file_reader = instruments.get(instrument, None)(serial)
        try:
            file_exts = ("csv", "DATA", "DAT", "data")
            if file.filename.split(".")[-1] in file_exts:
                df = process_measurement_file(file, file_reader)
                df["instrument_serial"] = file_reader.serial
                df["instrument_model"] = file_reader.model
                in_rows = len(df)
                df["datetime"] = (
                    df["datetime"]
                    .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                    .dt.tz_convert("UTC")
                )

                pushed_data, dupes = df_to_gas_table(df)
                logger.debug(pushed_data)
                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)

            if "zip" in file.filename:
                logger.debug("Process zip")
                df = process_measurement_zip(file, file_reader)
                in_rows = len(df)
                df["instrument_serial"] = file_reader.serial
                df["instrument_model"] = file_reader.model
                in_rows = len(df)
                df["datetime"] = (
                    df["datetime"]
                    .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                    .dt.tz_convert("UTC")
                )

                pushed_data, dupes = df_to_gas_table(df)
                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)
            return {
                "message": f"Pushed {row_count}/{in_rows} gas measurements to db.",
            }, 200
        except Exception as e:
            return {"message": f"Unable to parse {file}, exception: {e}"}, 500


class CycleApi(Resource):
    def get(self):
        pass

    @login_required
    def post(self):
        """
        Upload a CSV file and process it.
        """
        # Check if a file is provided
        if "file" not in request.files:
            return {"message": "No file provided"}, 400

        file = request.files["file"]

        # Check if the file has a valid filename
        if file.filename == "":
            return {"message": "No selected file"}, 400

        try:
            if "csv" in file.filename:
                df = pd.read_csv(file)
                df["start_time"] = pd.to_datetime(df["start_time"], format="ISO8601")
                df["start_time"] = (
                    df["start_time"]
                    .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                    .dt.tz_convert("UTC")
                )
                print(df)
                in_cycles = len(df)
                pushed_data = df_to_cycle_table(df)

                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)

            # Read the CSV into a Pandas DataFrame
            if "log" in file.filename:
                df = process_protocol_file(file, chamber_map)
                in_cycles = len(df)

                pushed_data = df_to_cycle_table(df)
                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)

            if "zip" in file.filename:
                df = process_protocol_zip(file, chamber_map)
                in_cycles = len(df)
                pushed_data = df_to_cycle_table(df)
                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)
            return {
                "message": f"Pushed {row_count}/{in_cycles} cycles to db.",
            }, 200
        except Exception as e:
            return {"message": f"Unable to parse {file}, error {e}"}, 500
        pass


class InitFluxApi(Resource):
    def get(self):
        pass

    @login_required
    def post(self):
        def generate(df):
            chunk_size = 10  # Number of rows to process per chunk
            total_len = len(df)

            for start_idx in range(0, total_len, chunk_size):
                # Get the chunk of rows
                chunk_df = df.iloc[start_idx : start_idx + chunk_size]

                # Process the chunk
                start = chunk_df["start_time"].iloc[0]
                end = chunk_df["start_time"].iloc[0]
                init_from_cycle_table(chunk_df, None, None)

                # Yield progress update
                yield f"Step {min(start_idx + chunk_size, total_len)}/{total_len} completed between {start} - {end}\n"

        json = request.get_json()
        start = json.get("start", None)
        end = json.get("end", None)
        if start is None:
            return {"message": "No start date given."}
        if end is None:
            return {"message": "No end date given."}
        try:
            pd.to_datetime(start, format="ISO8601")
        except Exception:
            return {"message": "Give start date in proper format"}

        try:
            pd.to_datetime(end, format="ISO8601")
        except Exception:
            return {"message": "Give end date in proper format"}

        fluxes = flux_table_to_df()
        dupes = set(fluxes["start_time"])
        with engine.connect() as conn:
            df = cycle_table_to_df(start, end, conn)
            if df.empty or df is None:
                return {"message": f"No cycles between {start} and {end}."}, 200
            logger.debug(df)
            df = df[~df["start_time"].isin(dupes)]
            if df.empty or df is None:
                return {
                    "message": f"All cycles initiated between {start} and {end}."
                }, 200
            df.sort_values("start_time", inplace=True)
            logger.debug(df)
        return Response(generate(df), content_type="text/plain")


class MeteoApi(Resource):
    def get(self):
        pass

    @login_required
    def post(self):
        # Check if a file is provided
        if "file" not in request.files:
            return {"message": "No file provided"}, 400

        file = request.files["file"]

        # Check if the file has a valid filename
        if file.filename == "":
            return {"message": "No selected file"}, 400

        data = request.get_json()
        source = data.get("source", None)

        def read_meteo_file(file):
            df = pd.read_csv(file)
            df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")
            df["datetime"] = (
                df["datetime"]
                .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                .dt.tz_convert("UTC")
            )
            df["source"] = source
            return df

        try:
            if "csv" in file.filename:
                df = read_meteo_file(file)
                in_cycles = len(df)
                pushed_data = df_to_meteo_table(df)

                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)

            return {
                "message": f"Pushed {row_count}/{in_cycles} cycles to db.",
            }, 200
        except Exception as e:
            return {"message": f"Unable to parse {file}, error {e}"}, 500
        pass


@login_manager.unauthorized_handler
def unauthorized_callback():
    # Check if the request is from an API (Accept: application/json or path)
    if request.accept_mimetypes.best == "application/json" or request.path.startswith(
        "/api/"
    ):
        response = jsonify({"message": "Unauthorized access. Please log in."})
        response.status_code = 401
        return response
    else:
        # Redirect for non-API requests (e.g., web pages)
        return redirect(url_for("/login/"))
