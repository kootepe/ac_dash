import time
from plotly.graph_objs import Scattergl
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from pprint import pprint
from collections import namedtuple
from .tools.influxdb_funcs import just_read, read_ifdb
from .tools.gas_funcs import (
    calculate_pearsons_r,
    calculate_gas_flux,
    calculate_slope,
)
from .data_mgt import get_single_meteo
from .validation import check_valid_early, check_valid_deferred
from .data_mgt import (
    gas_table_to_df,
    flux_to_df,
    single_flux_to_table,
    get_single_volume,
)
from .tools.filter import get_datetime_index
from .validation import parse_error_codes, error_codes
from .measuring import instruments
import logging

logger = logging.getLogger("defaultLogger")


class MeasurementCycle:
    def __init__(
        self,
        id,
        start,
        close_offset,
        open_offset,
        end_offset,
        instrument,
        data=None,
        conn=None,
        meteo_source=None,
    ):
        self.id = id
        self.instrument = instrument
        self.flux_gases = self.instrument.flux_gases
        self.gases = self.instrument.gases
        self.start_time = start
        self.data = data
        self._close_offset = close_offset
        self._open_offset = open_offset
        self._end_offset = end_offset

        self._has_errors = False
        self.has_data = True
        self._error_code = 0
        self._error_string = ""
        self._is_valid_manual = None
        self.default_temperature = True
        self.default_pressure = True
        # NOTE: unused, could be used to find the end of the cycle when the
        # cycle extends beyond the start and end times
        self.end_extension = 0
        self.air_pressure = 1000
        self.air_temperature = 10
        self._chamber_height = 1
        self.default_height = True
        self.checks_str = ""
        self.max_r_ran = 0
        self.got_lag = None
        self.lagtime = 0
        # self.og_open = open
        self.lag_end = self.open + pd.Timedelta(seconds=160)
        self.all_r_ch4 = [0]
        self.updated_height = False
        # init from db if data found
        if self.check_db():
            self.lag_end = self.open + pd.Timedelta(seconds=160)
            if self.updated_height:
                for gas in self.flux_gases:
                    self.calculate_flux(gas)
                    self.updated_height = False
                single_flux_to_table(self.get_attribute_df())

            return
        # BUG: this assumes that a row always has both temp and pressure
        self.air_temperature, self.air_pressure = get_single_meteo(
            self.start_time, meteo_source
        )
        self.chamber_height = get_single_volume(self.start_time)
        logger.debug("No flux in db")

        # used to look for the drop indicating the opening of the chamber for
        # lag time check as we don't need to process the whole dataframe.
        self.calc_data = None
        self.got_lag = None
        self._is_valid = True
        self._is_valid_manual = None
        self.no_data_in_db = False
        self.adjusted_time = False
        self.manual_invalid = False

        self.slope = {gas: 1 for gas in self.flux_gases}
        self.flux = {gas: 0 for gas in self.flux_gases}
        self.r = {gas: 0 for gas in self.flux_gases}
        self.r2 = {gas: 0 for gas in self.flux_gases}
        self.r_offset = {gas: 0 for gas in self.flux_gases}
        self.calc_offset_s = {gas: 0 for gas in self.flux_gases}
        self.calc_offset_e = {gas: 0 for gas in self.flux_gases}

        self.quality_r = 1
        self.quality_r2 = 1
        self.get_max(conn=conn)

    def manual_lag(self, lag):
        return self.get_max(manual_lag=lag)

    @property
    def error_code(self):
        return self._error_code

    @error_code.setter
    def error_code(self, value):
        if not isinstance(value, (int, np.int64, np.int32)):
            raise ValueError("Error code must be integer.")

        logger.info(value)
        if value == 0:
            self._error_code = 0  # Reset error code
            self.error_string = ""
            self.is_valid = True
            return
        if value > 0:
            self.is_valid = False
            # self._error_code += 1024
        self._error_code = self.set_error_code(value)
        self.error_string = ", ".join(parse_error_codes(self._error_code, error_codes))
        logger.info(self.error_code)
        logger.info(self.error_string)

    @property
    def error_string(self):
        return self._error_string

    @error_string.setter
    def error_string(self, value):
        if not isinstance(value, str):
            raise ValueError("error_string must be string")
        self._error_string = value

    @property
    def open(self):
        return (
            self.start_time
            + pd.Timedelta(seconds=self._open_offset)
            + pd.Timedelta(seconds=self.lagtime)
        )

    @property
    def close(self):
        return (
            self.start_time
            + pd.Timedelta(seconds=self._close_offset)
            + pd.Timedelta(seconds=self.lagtime)
        )

    @property
    def og_close(self):
        return self.start_time + pd.Timedelta(seconds=self._close_offset)

    @property
    def og_open(self):
        return self.start_time + pd.Timedelta(seconds=self._open_offset)

    @property
    def open_offset(self):
        return self._open_offset

    @open_offset.setter
    def open_offset(self, value):
        if isinstance(value, (int, np.int64)):
            self._open_offset = value

    @property
    def end(self):
        return self.start_time + pd.Timedelta(seconds=self._end_offset)

    @property
    def attribute_dict(self):
        return self.get_attribute_dict()

    @property
    def attribute_df(self):
        return self.get_attribute_df()

    @property
    def chamber_height(self):
        return self._chamber_height

    @chamber_height.setter
    def chamber_height(self, value):
        if value is None:
            self.chamber_height = 1
            self.default_height = True
            return
        if isinstance(value, (float, int, np.int64, np.float64)):
            self._chamber_height = value
            self.default_height = False
        else:
            raise ValueError("chamber_height must be int or float")

    @property
    def is_valid_manual(self):
        return self._is_valid_manual

    @is_valid_manual.setter
    def is_valid_manual(self, value):
        if isinstance(value, bool):
            self._is_valid_manual = value
            self._is_valid = value
            if value is False:
                self.error_code += 1024
            if value is True:
                self.error_code = 0
            logger.info(f"set manual_is_valid to {value}")

        else:
            raise ValueError("is_valid_manual must be a boolean.")

    @property
    def has_errors(self):
        return self._has_errors

    @has_errors.setter
    def has_errors(self, value):
        if isinstance(value, bool):
            self._has_errors = value
            if value is False:
                self.is_valid = False
        else:
            raise ValueError("has_errors must be a boolean.")

    @property
    def air_pressure(self):
        return self._air_pressure

    @air_pressure.setter
    def air_pressure(self, value):
        if value is None:
            self._air_pressure = 1000
            self.default_pressure = True
            return

        if isinstance(value, (int, np.int64, float, np.float64)):
            self._air_pressure = value
            self.default_pressure = False
        else:
            raise ValueError("air_pressure must be int or float.")

    @property
    def air_temperature(self):
        return self._air_temperature

    @air_temperature.setter
    def air_temperature(self, value):
        if value is None:
            self._air_temperature = 1000
            self.default_temperature = True
            return

        if isinstance(value, (int, np.int64, float, np.float64)):
            self._air_temperature = value
            self.default_temperature = False
        else:
            raise ValueError("air_temperature must be int or float.")

    @property
    def is_valid(self):
        return self._is_valid

    @is_valid.setter
    def is_valid(self, value):
        if isinstance(value, bool):
            self._is_valid = value
        else:
            raise ValueError("is_valid must be a boolean.")

    def set_error_code(self, error_code):
        """Set only the bits in the error code that are not already set.

        Returns:
            int: The new bits added to the error code (or 0 if no new bits are added).
        """
        new_bits = (
            error_code & ~self._error_code
        )  # Identify bits that are not already set
        if new_bits:  # If there are new bits to add
            self._error_code |= new_bits  # Set the new bits using bitwise OR
        return new_bits

    def get_attribute_df(self):
        data = {key: [item] for key, item in self.attribute_dict.items()}
        return pd.DataFrame(data)

    def get_attribute_dict(self):
        slopes = {f"{gas}_slope": self.slope.get(gas) for gas in self.flux_gases}
        fluxes = {f"{gas}_flux": self.flux.get(gas) for gas in self.flux_gases}
        rs = {f"{gas}_r": self.r.get(gas) for gas in self.flux_gases}
        r2s = {f"{gas}_r2": self.r.get(gas) for gas in self.flux_gases}
        offset_s = {
            f"{gas}_offset_s": self.calc_offset_s.get(gas) for gas in self.flux_gases
        }
        offset_e = {
            f"{gas}_offset_e": self.calc_offset_e.get(gas) for gas in self.flux_gases
        }
        attributes = {
            "start_time": self.start_time,
            "chamber_id": self.id,
            "instrument_model": self.instrument.model,
            "instrument_serial": self.instrument.serial,
            "updated_height": self.updated_height,
            "close_offset": self.close_offset,
            "open_offset": self.open_offset,
            "end_offset": self.end_offset,
            "air_pressure": self.air_pressure,
            "air_temperature": self.air_temperature,
            "lagtime": self.lagtime,
            "quality_r": float(self.quality_r),
            "quality_r2": float(self.quality_r2),
            "is_valid": self._is_valid,
            "error_code": self.error_code,
            "chamber_height": self.chamber_height,
            **slopes,
            **fluxes,
            **rs,
            **r2s,
            **offset_s,
            **offset_e,
        }
        # logger.debug(attributes)
        return attributes

    @property
    def close_offset(self):
        # return (self.close - self.start_time).total_seconds()
        return self._close_offset

    @close_offset.setter
    def close_offset(self, value):
        self._close_offset = value

    @property
    def end_offset(self):
        return (self.end - self.start_time).total_seconds()

    @end_offset.setter
    def end_offset(self, value):
        self._end_offset = value

    def del_lagtime(self):
        self.got_lag = False
        self.lagtime = 0

    def validity_checks(self):
        pass

    def check_db(self, conn=None):
        """
        Initiate measurement from the db representation
        """
        df = flux_to_df(self.start_time, self.instrument.serial, conn)
        if df is None or df.empty:
            return False
        vals = df.iloc[0]

        start = vals.get("start_time")

        instrument_serial = vals.get("instrument_serial")
        instrument_model = vals.get("instrument_model")
        self.instrument = instruments.get(instrument_model.replace("-", ""))(
            instrument_serial
        )
        self.start_time = start
        self.lagtime = vals.get("lagtime")
        self._close_offset = vals.get("close_offset")
        self._open_offset = vals.get("open_offset")
        self._end_offset = vals.get("end_offset")
        self.updated_height = vals.get("updated_height")

        self.id = vals.get("chamber_id")
        self.end_offset = vals.get("end_offset")
        self.air_pressure = vals.get("air_pressure")
        self.air_temperature = vals.get("air_temperature")
        self.quality_r = vals.get("quality_r")
        self.quality_r2 = vals.get("quality_r2")
        self._is_valid = bool(vals.get("is_valid"))
        self.error_code = vals.get("error_code")
        if vals.get("height_unit") is not None:
            self.height_unit = vals.get("height_unit")
        if vals.get("chamber_height") is not None:
            self.chamber_height = vals.get("chamber_height", 1)
        setattr(
            self, "flux", {f"{gas}": vals.get(f"{gas}_flux") for gas in self.flux_gases}
        )
        setattr(self, "r", {f"{gas}": vals.get(f"{gas}_r") for gas in self.flux_gases})
        setattr(
            self, "r2", {f"{gas}": vals.get(f"{gas}_r2") for gas in self.flux_gases}
        )
        setattr(
            self,
            "slope",
            {f"{gas}": vals.get(f"{gas}_slope") for gas in self.flux_gases},
        )
        setattr(
            self,
            "calc_offset_s",
            {f"{gas}": vals.get(f"{gas}_offset_s") for gas in self.flux_gases},
        )
        setattr(
            self,
            "calc_offset_e",
            {f"{gas}": vals.get(f"{gas}_offset_e") for gas in self.flux_gases},
        )
        self.data = gas_table_to_df(self.start_time, self.end)
        self.calc_data = gas_table_to_df(self.close, self.open)
        self.check_no_data()

        # logger.debug(self.data)
        return True

        # for key, item in values.items():
        #     print(f"{key}: {item}")

        # logger.debug(df)

    def from_df(self, df):
        pass

    def init_measurements(self, all_data):
        logger.debug(f"Initiating measurement at  {self.start_time}")
        logger.debug(self.start_time)
        logger.debug(self.end)
        start, end = get_datetime_index(all_data, self)
        self.data = all_data.iloc[start:end].copy()
        if self.check_no_data():
            self.has_data = False
            return

        data_len = len(self.data)
        expected_len = (self.end - self.start_time).total_seconds()
        logger.debug(f"Got data dataframe of length {data_len}")
        if data_len < expected_len * 0.5:
            self.is_valid = False
            return

        self.error_code += check_valid_early(self)
        if self.is_valid is False:
            return
        logger.debug("Setting index")
        logger.debug("Getting calc dataframe")
        close, open = get_datetime_index(self.data, self, s_key="close", e_key="open")
        self.calc_data = self.data.iloc[close:open].copy()
        expected_calc_len = (self.open - self.close).total_seconds()
        if len(self.calc_data) < expected_calc_len * 0.5:
            self.is_valid = False
            return

        logger.debug(f"Got calc dataframe length {len(self.calc_data)}")
        self.get_max()
        self.error_code = 0
        self.error_code += check_valid_deferred(self)

    def get_data(self, ifdb_dict, conn=None):
        if self.data is None or self.data.empty:
            logger.debug(f"Getting data from {self.start_time} to {self.end}")
            self.data = gas_table_to_df(self.start_time, self.end, conn)
            if self.data is None or self.data.empty:
                return
            logger.debug(f"Data length {len(self.data)}")
            # self.data.index = pd.to_datetime(self.data["datetime"])

            self.error_code += check_valid_early(self)
            if self.is_valid is False:
                return

            # self.data.set_index("datetime", inplace=True)
            logger.debug(self.close)
            logger.debug(self.open)
            start, end = get_datetime_index(
                self.data, self, s_key="close", e_key="open"
            )
            self.calc_data = self.data.iloc[start:end]

    def get_max(self, ifdb_dict=None, manual_lag=None, conn=None):
        logger.debug("Running get_max")
        data = None
        if self.data is None:
            self.get_data(ifdb_dict, conn)
            if self.data is None or self.data.empty:
                return
        else:
            logger.debug("Data is not None")

        if self.data is not None and not self.data.empty:
            pass
        if manual_lag:
            self.lagtime = manual_lag
        if self.lagtime == 0:
            self.get_lagtime()
        start, end = get_datetime_index(self.data, self, s_key="close", e_key="open")
        self.calc_data = self.data.iloc[start:end].copy()
        expected_len = (self.start_time - self.close).total_seconds() * 0.9
        if expected_len > len(self.calc_data):
            return
        # for gas in ["CH4", "CO2"]:
        for gas in self.flux_gases:
            self.get_max_r(gas)
            self.calculate_flux(gas)

        # calculate std of all calculated ch4 r values to drop bad measurements
        r_vals = np.array(self.all_r_ch4)

        self.error_code = 0
        self.error_code += check_valid_deferred(self)
        logger.info(self.error_code)

    def get_lagtime(self):
        if self.has_errors is True:
            self.lagtime = 0
        if self.got_lag is True:
            return
        self.got_lag = True
        self.lagtime = 0

        start, end = get_datetime_index(self.data, self, s_key="open", e_key="lag_end")
        data = self.data.iloc[start:end].copy()
        if data.empty:
            logger.debug("No data for finding lag")
            return
        logger.debug(f"lag_end dataframe length {len(data)}")

        lagtime_idx = data["CH4"].idxmax()
        logger.debug(lagtime_idx)
        lagtime_idx = self.find_negative_lagtime(data, lagtime_idx)
        # if (lagtime_idx - open).total_seconds() == 119:
        if (lagtime_idx - self.open).total_seconds() >= 100:
            logger.debug("Found max lag")
            lagtime_idx = self.find_negative_lagtime(data, lagtime_idx, 10)
            logger.debug(lagtime_idx)

        self.lagtime = int((lagtime_idx - self.og_open).total_seconds())

    def find_negative_lagtime(self, data, lagtime_idx, back=0):
        logger.debug("Trying to find negative lagtime")
        repeats = 0
        open = self.og_open
        lag_end = self.lag_end + pd.Timedelta(minutes=self.end_extension)
        back = pd.Timedelta(seconds=back)
        lag = (lagtime_idx - open).total_seconds()
        while (lag == 0 and repeats < 12) or (lag >= 110 and repeats < 10):
            logger.debug(lag)
            repeats += 1
            ten_s = pd.Timedelta(seconds=10) + back
            start = open - ten_s
            end = lag_end - ten_s
            logger.debug(f"Find between {start} {end} ")
            data = self.get_lag_df(start, end).copy()
            if data.empty:
                continue
            lagtime_idx = data["CH4"].idxmax()
            logger.debug(lagtime_idx)
            open = open - ten_s
            lag_end = lag_end - ten_s

        return lagtime_idx

    def get_lag_df(self, start, end):
        frame = namedtuple("filter", ["open", "lag_end"])
        filter = frame(start, end)

        start, end = get_datetime_index(
            self.data, filter, s_key="open", e_key="lag_end"
        )
        data = self.data.iloc[start:end].copy()
        return data

    def push_lagtimes(self, ifdb_dict):
        pass

    def calculate_flux(self, gas):
        logger.debug(f"Calculating {gas} flux.")

        flux = 0
        slope = 0
        if self.data is None or self.data.empty:
            return
        self.s = self.start_time + pd.Timedelta(seconds=self.calc_offset_s.get(gas))
        self.e = self.start_time + pd.Timedelta(seconds=self.calc_offset_e.get(gas))
        logger.debug(self.s)
        logger.debug(self.e)
        logger.debug(f"Data length: {len(self.data)}")
        start, end = get_datetime_index(self.data, self, s_key="s", e_key="e")
        data = self.data.iloc[start:end].copy()

        logger.debug(f"Flux calculation data length: {len(data)}")
        nullcheck = data[gas].isnull().values.all()
        if data.empty or len(data) == 0 or nullcheck:
            slope = 0
            flux = 0
            r = 0
        else:
            slope = calculate_slope(data.index.astype(int) // 10**9, data[gas])
            r = calculate_pearsons_r(data.index.view(int), data[gas])
            logger.debug(self.calc_data)
            flux = calculate_gas_flux(self, gas, slope, self.chamber_height)
            if gas == "CH4":
                start, end = get_datetime_index(
                    self.data, self, s_key="close", e_key="open"
                )
                new_data = self.data.iloc[start:end].copy()
                self.quality_r = calculate_pearsons_r(
                    new_data.index.view(int), new_data[gas]
                )
                self.quality_r2 = self.quality_r**2
        if self.has_errors:
            flux = 0
        if not flux:
            flux = 0
        flux = round(flux, 10)
        slope = round(slope, 10)
        self.slope[gas] = slope
        self.flux[gas] = flux
        self.r[gas] = r
        self.r2[gas] = r**2

        logger.debug(f"{gas} flux: {flux}")

    def get_max_r(self, gas):
        self.max_r_ran += 1
        logger.debug(f"Running get_max_r for time {self.max_r_ran}")

        df = self.calc_data.copy()
        logger.debug(f"Data length: {len(df)}")
        if df.empty:
            logger.debug("No data for calc")
            return

        second_sweep = False
        increment_seconds = 15
        increment_delta = pd.Timedelta(seconds=increment_seconds)
        min_interval_seconds = 120
        # set max interval seconds to calculation data length so that we dont go
        # outside it
        max_interval_seconds = int((self.open - self.close).total_seconds())
        max_r, max_r_idx_s, max_r_idx_e = None, None, None
        all_r = []

        # set to track processed periods
        processed = set()
        start_time = df.index[0]
        end_time = min(df.index[-1], self.open)

        # Function to evaluate intervals for a single forward sweep
        def sweep_intervals(start_time, min_interval):
            nonlocal max_r, max_r_idx_s, max_r_idx_e, all_r, processed

            interval_start = start_time
            while interval_start + pd.Timedelta(seconds=min_interval) <= end_time:
                for interval_duration in range(
                    min_interval,
                    max_interval_seconds,
                    increment_seconds,
                ):
                    interval_delta = pd.Timedelta(seconds=interval_duration)
                    interval_end = interval_start + interval_delta

                    if interval_end > end_time:
                        interval_end = end_time
                        interval_start = interval_start + pd.Timedelta(
                            seconds=increment_seconds
                        )
                    key = (interval_start, interval_end)
                    if key in processed:
                        continue
                    if int((end_time - interval_start).total_seconds()) < min_interval:
                        continue
                    processed.add((interval_start, interval_end))

                    data = df[(df.index >= interval_start) & (df.index < interval_end)]
                    nullcheck = data[gas].isnull().values.all()
                    if data.empty or data is None or nullcheck:
                        continue

                    interval_seconds = (interval_end - interval_start).total_seconds()
                    missing_data = len(data) < (interval_seconds * 0.9)

                    if not missing_data:
                        r = calculate_pearsons_r(data.index.view(int), data[gas])
                        all_r.append(r)

                        if len(data) >= min_interval and (max_r is None or r > max_r):
                            max_r, max_r_idx_s, max_r_idx_e = (
                                r,
                                data.index[0],
                                data.index[-1],
                            )

                interval_start += increment_delta

        # Perform forward sweeps with staggered starts
        logger.debug("first sweep")
        for offset in range(0, min_interval_seconds, increment_seconds):
            sweep_intervals(
                start_time + pd.Timedelta(seconds=offset), min_interval_seconds
            )
        # logger.info(max_r)
        # logger.info((max_r_idx_e - max_r_idx_s).total_seconds())

        # measurements in early spring can be very noisy and 120 seconds doesnt
        # get a realistic value for r, try and increase the searching period
        # a bit artificially here
        # NOTE: find a way to classify noisy and oscillating measurements so we can
        # avoid doing this slow loop again
        if max_r is not None and second_sweep is False and max_r < 0.5:
            logger.debug("Second sweep")
            second_sweep = True
            max_r = None
            new_min_interval_seconds = int(max_interval_seconds * 0.7)
            for offset in range(0, new_min_interval_seconds, increment_seconds):
                sweep_intervals(
                    start_time + pd.Timedelta(seconds=offset), new_min_interval_seconds
                )

        if max_r_idx_s is None:
            max_r_idx_s = self.start_time
            max_r_idx_e = self.start_time + pd.Timedelta(seconds=180)
            max_r = 1
            self.is_valid = False
            logger.debug("No valid maximum R found.")
        elif pd.isna(max_r) or max_r < 0.1:
            max_r = 1

        max_r_offset_s = int((max_r_idx_s - self.start_time).total_seconds())
        max_r_offset_e = int((max_r_idx_e - self.start_time).total_seconds())

        logger.debug(
            f"Max R: {max_r}, Offset Start: {max_r_offset_s}, Offset End: {max_r_offset_e}"
        )
        logger.debug(max_r_offset_e)
        logger.debug(max_r_offset_s)
        logger.debug(f"Calculated {len(all_r)} values of r.")

        self.r[gas] = max_r
        self.r2[gas] = max_r**2
        self.calc_offset_s[gas] = max_r_offset_s
        self.calc_offset_e[gas] = max_r_offset_e

    def calculate_r(self, gas):
        self.s = self.start_time + pd.Timedelta(seconds=self.calc_offset_s.get(gas))
        self.e = self.start_time + pd.Timedelta(seconds=self.calc_offset_e.get(gas))
        logger.debug(f"Data length: {len(self.data)}")
        start, end = get_datetime_index(self.data, self, s_key="s", e_key="e")
        data = self.data.iloc[start:end].copy()
        if data.empty:
            r = 0

        r = calculate_pearsons_r(data.index.view(int), data[gas])
        self.r[gas] = r
        self.r2[gas] = r**2

    def mk_gas_plot(self, gas, color_key="blue", zoom_to_calc=0):
        logger.debug(f"Running for {gas}.")
        color_dict = {"blue": "rgb(14,168,213,0)", "green": "rgba(27,187,11,1)"}
        logger.debug(self.data)
        if self.data[gas].empty or self.data is None or self.data[gas].isnull().all():
            return self.return_invalid(go.Figure())

        close = self.close
        s_offset = self.calc_offset_s.get(gas)
        e_offset = self.calc_offset_e.get(gas)

        r_s = self.start_time + pd.Timedelta(seconds=s_offset)
        if r_s < self.close:
            r_s = self.close

        r_e = self.start_time + pd.Timedelta(seconds=e_offset)
        if r_e > self.open:
            r_e = self.open

        trace_data = go.Scattergl(
            x=self.data.index,
            y=self.data[gas],
            mode="markers",
            name="Data",
            marker=dict(
                color="rgba(65,224,22,0)",
                symbol="x-thin",
                size=5,
                line=dict(color=color_dict.get(color_key), width=1),
            ),
        )
        og_close = go.Scattergl(
            x=[self.og_close, self.og_close],
            y=[
                self.data[gas].min() - 100000,
                self.data[gas].max() + 100000,
            ],
            mode="lines",
            opacity=0.2,
            line=dict(color="red", dash="solid"),
            name="Unadjusted close",
        )
        og_open = go.Scattergl(
            x=[self.og_open, self.og_open],
            y=[
                self.data[gas].min() - 100000,
                self.data[gas].max() + 100000,
            ],
            mode="lines",
            opacity=0.2,
            line=dict(color="green", dash="solid"),
            name="Unadjusted open",
        )
        close_line = go.Scattergl(
            x=[close, close],
            y=[
                self.data[gas].min() - 100000,
                self.data[gas].max() + 100000,
            ],
            mode="lines",
            line=dict(color="red", dash="dash"),
            name="Adjusted close",
        )

        layout = go.Layout(
            title={
                "text": f"Chamber {self.id} {gas} Measurement {self.start_time}",
                "font": {"family": "monospace", "size": 14},
            },
            margin=dict(
                l=10,
                r=10,
                t=25,
                b=10,
            ),
            legend=dict(
                font=dict(size=13),
                orientation="v",
                tracegroupgap=3,
                # itemclick=False,
                # itemdoubleclick=False,
            ),
            xaxis=dict(type="date"),
            autosize=True,
        )
        dummy = go.Scattergl(
            x=[None],  # No actual data points
            y=[None],
            mode="lines",
            line=dict(color="black", width=2, dash="dash"),
            name="Movable lagtime / close",  # Legend entry name
        )

        fig = go.Figure(
            # data=([trace_data, og_open, og_close, dummy, open_line, close_line]),
            data=([trace_data, og_open, og_close, dummy, close_line]),
            layout=layout,
        )
        fig.add_shape(
            type="line",
            name="lag-line",
            x0=self.open,
            x1=self.open,
            # x0=self.open + pd.Timedelta(seconds=self.lagtime),
            # x1=self.open + pd.Timedelta(seconds=self.lagtime),
            y0=min(self.data[gas] - 100000),
            y1=max(self.data[gas] + 100000),
            line_dash="dash",
            line_width=2,
        )
        fig.add_shape(
            type="rect",
            name="r-poly",
            x0=r_s,
            x1=r_e,
            y0=min(self.data[gas] - 100000),
            y1=max(self.data[gas] + 100000),
            fillcolor="lightslategrey",
            line_color="black",
            opacity=0.3,
            line_width=2,
        )
        yrange = self.data[gas].max() - self.data[gas].min()
        yrange_perc = yrange * 0.1
        fig.update_yaxes(
            autorangeoptions_maxallowed=self.data[gas].max() + yrange_perc,
            autorangeoptions_minallowed=self.data[gas].min() - yrange_perc,
            scaleanchor=False,
        )
        # NOTE: logic to zoom the graph to the calculation data, making it
        # easier to see whats happening in some measurements where there is a very
        # high initial rise of concentration.
        # NOTE: add a dcc.Store for user settings so that app doesnt need to be
        # restarted to apply these
        if zoom_to_calc == 1:
            yrange = self.calc_data[gas].max() - self.calc_data[gas].min()
            yrange_perc = yrange * 0.1
            fig.update_yaxes(
                autorangeoptions_maxallowed=self.calc_data[gas].max() + yrange_perc,
                autorangeoptions_minallowed=self.calc_data[gas].min() - yrange_perc,
                scaleanchor=False,
            )
        if (
            self.is_valid is False
            or self._is_valid is False
            or self.has_errors is True
            or self.is_valid_manual is False
        ):
            logger.debug(f"measurement.is_valid: {self.is_valid}")
            logger.debug(f"measurement.has_errors: {self.has_errors}")
            logger.debug(f"measurement.is_valid_manual: {self.is_valid_manual}")
            fig.update_layout(
                {
                    "plot_bgcolor": "rgba(255, 223, 223, 1)",
                },
                annotations=[
                    dict(
                        name="draft watermark",
                        text="",
                        textangle=0,
                        opacity=0.4,
                        font=dict(color="black", size=50),
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                        showarrow=False,
                    )
                ],
            )

        return fig

    def return_invalid(self, fig):
        return fig.update_layout(
            {
                "plot_bgcolor": "rgba(255, 223, 223, 1)",
            },
            annotations=[
                dict(
                    name="draft watermark",
                    text="",
                    textangle=0,
                    opacity=0.4,
                    font=dict(color="black", size=50),
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                )
            ],
        )
        pass

    def check_no_data(self):
        if self.data is None or self.data.empty:
            logger.debug("No data.")
            self.is_valid = False
            self.checks_str += "no_data,"
            self.has_data = False
            return True
        else:
            return False

    def __repr__(self):
        formatted_lag = f"{self.lagtime:4}"
        html_lag = formatted_lag.replace(" ", "\u00a0")
        formatted_h = f"{self.chamber_height:4}"
        html_h = formatted_h.replace(" ", "\u00a0")
        return f"{self.start_time}, height: {html_h}m, lag: {html_lag}s, temp: {self.air_temperature:.2f}c, error flags: {self.error_string} "
