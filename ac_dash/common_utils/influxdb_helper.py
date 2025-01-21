from abc import ABC, abstractmethod
from pprint import pformat
import logging
import influxdb_client as ifdb
from datetime import timedelta

from influxdb_client.client.write_api import SYNCHRONOUS
from urllib3.exceptions import NewConnectionError

from ..common_utils.utils import LOCAL_TZ

logger = logging.getLogger("defaultLogger")


class IfdbPoint(ABC):
    def __init__(self, measurement, tags, fields, time):
        self.measurement = measurement
        self.tags = tags
        self.fields = fields
        self.time = time

    @abstractmethod
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

    @abstractmethod
    def get_local_time(self):
        return self.time.astimezone(LOCAL_TZ)

    @abstractmethod
    def __str__(self):
        """
        return pretty string representation
        """
        return pformat(self.to_dict())


def push_ifdbpoint(point, ifdb_dict):
    """
    Push data to InfluxDB

    args:
    ---
    df -- pandas dataframe
        data to be pushed into influxdb

    returns:
    ---

    """
    url = ifdb_dict.get("url")
    bucket = ifdb_dict.get("bucket")
    point_data = point.to_dict()

    with init_client(ifdb_dict) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        logger.debug("Attempting point write.")
        logger.debug(point)
        try:
            write_api.write(
                bucket=bucket,
                record=point_data,
                debug=True,
            )
        except NewConnectionError:
            print(f"Couldn't connect to database at {url}")

        logging.info("Pushed data between log item to DB")


def ifdb_push(point_data, ifdb_dict):
    """
    Push data to InfluxDB

    args:
    ---
    df -- pandas dataframe
        data to be pushed into influxdb

    returns:
    ---

    """
    url = ifdb_dict.get("url")
    bucket = ifdb_dict.get("bucket")

    with init_client(ifdb_dict) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        logger.debug("Attempting point write.")
        logger.debug(point_data)
        logger.debug(ifdb_dict)
        try:
            write_api.write(
                bucket=bucket,
                record=point_data,
                debug=True,
            )
        except NewConnectionError:
            print(f"Couldn't connect to database at {url}")
            pass

        logging.info("Pushed data between log item to DB")


def init_client(ifdb_dict):
    url = ifdb_dict.get("url")
    token = ifdb_dict.get("token")
    org = ifdb_dict.get("organization")
    timeout = ifdb_dict.get("timeout")
    logger.debug(f"Inititate connection at {url}")

    client = ifdb.InfluxDBClient(url=url, token=token, org=org, timeout=timeout)
    return client


def delete_by_uuid(ifdb_dict, start, uuid, measurement=None):
    logger.debug(ifdb_dict)
    start_ts = start
    if measurement is None:
        predicate = f'uuid="{uuid}"'
    else:
        measurement = ifdb_dict.get("measurement")
        predicate = f'_measurement="{measurement}" and uuid="{uuid}"'

    with init_client(ifdb_dict) as client:
        del_api = client.delete_api()
        del_api.delete(
            start_ts,
            start_ts + timedelta(minutes=1),
            predicate,
            bucket=ifdb_dict.get("bucket"),
        )


def ifdb_read(ifdb_dict):
    with init_client(ifdb_dict) as client:
        q_api = client.query_api()
        pass
    return
