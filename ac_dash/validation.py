import logging
from .tools.filter import get_datetime_index

logger = logging.getLogger("defaultLogger")

error_codes = {
    1: "has errors",
    2: "no data",
    4: "no air_temperature",
    8: "no air_pressure",
    16: "too many measurements",
    32: "too few measurements",
    64: "high r std",
    128: "improper close",
    256: "few unique values",
    512: "null gas measurements",
    1024: "manual invalid",
    2048: "",
    4096: "",
    8192: "",
    16384: "",
    32768: "",
    65536: "",
    131072: "",
}


def check_r_std(r_std):
    std_threshold = 0.13
    logger.debug(f"r_std: {r_std}")
    return r_std > std_threshold


def check_nunique(data):
    # somewhat arbitrary number, if there's less than 10 unique values, mark
    # invalid
    nunique_threshold = 10
    return data.nunique() < nunique_threshold


def check_trends_up(data, threshold):
    max = data["CH4"].resample("20s").max()
    max_shifted = (
        data["CH4"]
        .resample("20s")
        .max()
        .shift(1)
        .fillna(data["CH4"].resample("20s").max().iloc[0])
    )

    trending_up = max >= max_shifted
    percentage_upward = trending_up.mean() * 100
    logger.debug(f"Upward percent: {percentage_upward}")

    # if 95% of values are only going upward, mark invalid
    return percentage_upward >= threshold


def check_trends_down(measurement):
    min = measurement.data["CH4"].resample("20s").min()
    min_shifted = (
        measurement.data["CH4"]
        .resample("20s")
        .min()
        .shift(1)
        .fillna(measurement.data["CH4"].resample("20s").min().iloc[0])
    )
    trending_down = min <= min_shifted
    percentage_downward = trending_down.mean() * 100
    logger.debug(f"Downward percent: {percentage_downward}")
    # if 95% of values are only going downward, mark invalid
    threshold = 95
    return percentage_downward >= threshold


def check_diag_col(df, device):
    return df["DIAG"].sum() != 0


def check_air_temperature(measurement):
    return (
        measurement.air_temperature in [False, None]
        or measurement.default_temperature is True
    )


def check_air_pressure(measurement):
    return (
        measurement.air_pressure in [False, None]
        or measurement.default_pressure is True
    )


def check_too_few(df, measurement_time):
    return (measurement_time * 0.9) > len(df)


def check_too_many(df, measurement_time):
    logger.debug(f"Length: {len(df)}")
    logger.debug(f"Measurement_time: {measurement_time}")
    return len(df) > measurement_time * 1.1


def check_gas_measurement(measurement):
    return measurement.calc_data[measurement.flux_gases].isna().all().any()


def check_valid_early(measurement, device=None):
    logger.debug("Checking validity")
    measurement_time = (measurement.end - measurement.start_time).total_seconds()

    has_errors = check_diag_col(measurement.data, device)
    no_air_temp = check_air_temperature(measurement)
    no_air_pressure = check_air_pressure(measurement)
    is_empty = measurement.data.empty
    no_data = measurement.data is None
    too_many = check_too_many(measurement.data, measurement_time)
    too_few = check_too_few(measurement.data, measurement_time)

    checks_val = 0
    if has_errors or no_air_temp or no_air_pressure or is_empty or too_many or too_few:
        # if has_errors:
        #     checks_val += 1
        #     logger.debug("has errors")
        if no_data or is_empty:
            checks_val += 2
            logger.debug("no data,")
        if no_air_temp:
            checks_val += 4
            logger.debug("no air temp,")
        if no_air_pressure:
            checks_val += 8
            logger.debug("no air pressure,")
        # if too_many:
        #     checks_val += 16
        #     logger.debug("too many measurements,")
        # if too_few and not no_data and not is_empty:
        #     checks_val += 32
        #     logger.debug("too few measurements,")

        if checks_val > 0:
            measurement.is_valid = False
    return checks_val


def check_valid_deferred(measurement, device=None):
    # NOTE: Should this be moved inside one of the existing loops?
    # This loops through everything again, would be better to have it in the
    # same loop where we calculate flux?
    logger.debug("Checking validity")
    measurement_time = (measurement.open - measurement.close).total_seconds()
    data = measurement.calc_data
    close, end = get_datetime_index(
        measurement.data, measurement, s_key="close", e_key="end"
    )
    data_after_close = measurement.data.iloc[close:end].copy()

    has_errors = check_diag_col(data, device)
    # NOTE: potential false flags
    didnt_close = check_trends_up(data_after_close, 100)
    is_empty = data.empty
    too_many = check_too_many(data, measurement_time)
    too_few = check_too_few(data, measurement_time)
    few_nunique = check_nunique(data["CH4"])
    high_r_std = check_r_std(measurement.r_std)
    missing_gas = check_gas_measurement(measurement)

    checks_val = 0
    if (
        has_errors
        or is_empty
        or too_many
        or too_few
        or few_nunique
        or high_r_std
        or didnt_close
    ):
        if has_errors:
            checks_val += 1
            logger.debug("has errors")
            return checks_val
        if is_empty:
            checks_val += 2
            logger.debug("no data,")
        if too_many:
            checks_val += 16
            logger.debug("too many measurements,")
        if too_few:
            checks_val += 32
            logger.debug("too few measurements,")
        if high_r_std:
            checks_val += 64
            logger.debug("high r_std")
        if didnt_close:
            checks_val += 128
            logger.debug("didnt close properly")
        if few_nunique:
            checks_val += 256
            logger.debug("Too few unique values")
        if missing_gas:
            checks_val += 512
            logger.debug("One more gas measurements have no values")

    return checks_val


def parse_error_codes(error_code, error_codes):
    """
    Parses an integer error code into a list of error descriptions.

    Args:
        error_code (int): The combined error code (sum of powers of 2).
        error_codes (dict): Dictionary mapping error codes to descriptions.

    Returns:
        list: A list of error descriptions corresponding to the error code.
    """
    # Extract all error codes that are powers of 2
    descriptions = []
    for code, description in error_codes.items():
        if error_code & code:  # Check if the code is part of the error_code
            descriptions.append(description)
    return descriptions
