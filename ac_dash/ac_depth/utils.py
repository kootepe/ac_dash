import pandas as pd
from sqlalchemy import select
from ..db import engine
import os
import json
import pytz
from datetime import datetime as dt
import logging
from pprint import pformat
from dash import html

from ..common_utils.utils import mk_uuid
from ..common_utils.influxdb_helper import IfdbPoint, init_client
from ..data_mgt import Volume_tbl  # Assuming this is your SQLAlchemy model

LOCAL_TZ = pytz.timezone("Europe/Helsinki")
CONTAINER_TZ = pytz.timezone("UTC")

logger = logging.getLogger("defaultLogger")


def mk_display_div(measurement, role):
    divs = []
    o = 1
    for meas in measurement:
        uuid = meas["tags"].get("uuid")
        id = meas["tags"].get("id")
        time = meas.get("time")
        local_time = time.astimezone(LOCAL_TZ)
        time = dt.strftime(time, "%Y-%m-%d %H:%M")
        outstring = f"chamber: {id}\n{local_time}\nidentifier: {uuid}\n{str(meas.get('fields'))}"
        divs.append(
            html.Div(
                [
                    html.Pre(outstring),
                    html.Button(
                        f"Remove {uuid}",
                        id={
                            "type": "del-button",
                            "id": uuid,
                            "index": o,
                            "time": time,
                            "chamber_id": id,
                        },
                        disabled=0 if role == "admin" else 1,
                    ),
                ],
                style={"border": "2px solid black", "padding": "5px"},
                id=uuid,
            )
        )

    return divs


def show_old_measurements(pts, role):
    logger.debug("Old measurements")
    measurements = []
    for pt in pts:
        vals = pt.to_dict()
        measurements.append(vals)
    sorted_measurements = sorted(measurements, key=lambda x: x["time"])[::-1]
    measurement_divs = mk_display_div(sorted_measurements, role)
    return measurement_divs


class IfdbDepthPoint(IfdbPoint):
    def __init__(
        self, time, id, nw, sw, ne, se, mid, has_snow, unit, uuid=None
    ):
        measurement = "ac_depth"
        calc_depth = (((nw + sw + ne + se) / 4) + mid) / 2
        if uuid is None:
            logger.debug("Generating new uuid")
            uuid = mk_uuid()
        tags = {"id": id, "uuid": uuid, "has_snow": has_snow}
        fields = {
            "nw": nw,
            "sw": sw,
            "ne": ne,
            "se": se,
            "mid": mid,
            "calc_height": calc_depth,
            "unit": unit,
        }
        super().__init__(measurement, tags, fields, time)

    def to_dict(self):
        """
        Convert point to InfluxDB-compatible dictionary format
        """
        return {
            "measurement": self.measurement,
            "tags": self.tags,
            "fields": self.fields,
            "time": self.time,
        }

    def get_local_time(self):
        return self.time.astimezone(LOCAL_TZ)

    def __str__(self):
        """
        return pretty string representation
        """
        return pformat(self.to_dict())

    @classmethod
    def from_influxdb_result(cls, result):
        """
        Class method to initialize an instance of IfdbDepthPoint from an InfluxDB result record.
        """
        logger.debug("Init")
        # Extract the values from the InfluxDB record
        points = []
        current_point = {}

        prev_uuid = None
        for table in result:
            logger.debug(prev_uuid)
            for record in table.records:
                time = record.get_time()
                uuid = record["uuid"]

                # Only capture 'id' and 'has_snow' on the first record for each new point
                if uuid != prev_uuid:
                    if prev_uuid is not None:
                        # Append the previous point before starting a new one
                        points.append(
                            IfdbDepthPoint(
                                time=current_point["time"],
                                id=current_point["id"],
                                nw=current_point.get("nw"),
                                sw=current_point.get("sw"),
                                ne=current_point.get("ne"),
                                se=current_point.get("se"),
                                mid=current_point.get("mid"),
                                has_snow=current_point["has_snow"],
                                uuid=current_point["uuid"],
                            )
                        )
                    # Reset current_point for the new time
                    logger.debug(f"New point detected at {time} {uuid}.")
                    current_point = {
                        "time": None,
                        "uuid": None,
                        "has_snow": None,
                        "id": None,
                        "nw": None,
                        "sw": None,
                        "ne": None,
                        "se": None,
                        "mid": None,
                    }
                    current_point["time"] = record.get_time()
                    current_point["id"] = record["id"]
                    current_point["uuid"] = record["uuid"]
                    current_point["has_snow"] = record["has_snow"]

                # Update the current_point dictionary with the relevant field value
                logger.debug("Getting field")
                if record.get_field() == "nw":
                    current_point["nw"] = record.get_value()
                elif record.get_field() == "sw":
                    current_point["sw"] = record.get_value()
                elif record.get_field() == "ne":
                    current_point["ne"] = record.get_value()
                elif record.get_field() == "se":
                    current_point["se"] = record.get_value()
                elif record.get_field() == "mid":
                    current_point["mid"] = record.get_value()

                prev_uuid = uuid

        # Append the last point after finishing the loop
        if prev_uuid is not None:
            points.append(
                IfdbDepthPoint(
                    time=current_point["time"],
                    id=current_point["id"],
                    nw=current_point.get("nw"),
                    sw=current_point.get("sw"),
                    ne=current_point.get("ne"),
                    se=current_point.get("se"),
                    mid=current_point.get("mid"),
                    has_snow=current_point["has_snow"],
                    uuid=current_point["uuid"],
                )
            )

        return points


def load_config():
    """Load configuration for InfluxDB."""
    filepath = os.path.abspath(os.path.dirname(__file__))
    with open(f"{filepath}/../config/config.json", "r") as f:
        config = json.load(f)
    return config


def parse_local_datetime(date_str):
    """Parse date and time strings into a localized datetime object."""
    datetime_str = f"{date_str}"
    return LOCAL_TZ.localize(dt.strptime(datetime_str, "%Y-%m-%d %H:%M"))


def query_log_point(measurement, ifdb_dict):
    bucket = ifdb_dict.get("bucket")
    with init_client(ifdb_dict) as client:
        start = "-30d"
        stop = "now()"
        query = f"""
        from(bucket: "{bucket}")
        |> range(start: {start}, stop: {stop})
        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        |> filter(fn: (r) => r["_field"] == "nw" or r["_field"] == "sw" or r["_field"] == "ne" or r["_field"] == "se" or r["_field"] == "mid" or r["_field"] == "calc_depth")
        """
        q_api = client.query_api()
        result = q_api.query(query)

    points = IfdbDepthPoint.from_influxdb_result(result)

    return points


def get_volume_data(start=None, end=None):
    if start is None:
        start = pd.to_datetime("1970-01-01")
    if end is None:
        end = pd.to_datetime("2040-01-01")
    query = select(Volume_tbl).where(
        Volume_tbl.c.datetime >= start, Volume_tbl.c.datetime <= end
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df.sort_values(by="datetime", inplace=True)
    return df
