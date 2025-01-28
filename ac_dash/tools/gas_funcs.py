#!/usr/bin/env python3

import numpy as np
import logging

logger = logging.getLogger("defaultLogger")

# molar masses
masses = {"CH4": 16, "CO2": 44, "H2O": 18, "N2O": 44}
# conversions to ppm
convs = {"CH4": 1000, "CO2": 1, "H2O": 1, "N2O": 1000}


def calculate_gas_flux(measurement, gas, slope, height):
    """
    Calculates gas flux

    args:
    ---
    df : pandas.dataframe
        dataframe with slope calculated.
    measurement_name : str
        name of the gas that flux is going to be calculated for

    returns:
    ---
    flux : numpy.array
        one column for the dataframe with the calculated gas
        flux
    """
    logger.debug("CALCULATING FLUX")
    logger.debug(measurement)
    logger.debug(gas)
    logger.debug(slope)
    logger.debug(height)
    # this value must in m
    h = height
    # molar_mass
    m = masses.get(gas)
    # value to convert to ppm
    conv = convs.get(gas)
    slope = slope
    # C temperature to K
    t = measurement.air_temperature + 273.15
    # t = 10 + 273.15
    # hPa to Pa
    p = measurement.air_pressure * 100
    # universal gas constant
    r = 8.314
    # convert slope from ppX/s to ppm/hour
    slope_ppmh = (slope / conv) * 60 * 60

    # flux in mg/m2/h
    flux = slope_ppmh / 1000000 * h * ((m * p) / (r * t)) * 1000
    # flux = (
    #     slope
    #     * (height / (22.4 * 10**-3 * (273.15 / (273.15 - measurement.air_temperature))))
    #     * 1
    # )

    return flux


def calculate_pearsons_r(x, y):
    """
    Calculates pearsons R for a measurement

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with ordinal time column and gas measurement column
    date : tuple
        Tuple with start time and end time that will be used to select rows to
        calculate pearsons R from.
    measurement_name : string
        name of the column slope is going to be calculated for.

    Returns
    -------
    pearsons_r : pd.Series
        Dataframe column with calculated pearsons R
    """

    pearsons_r = round(abs(np.corrcoef(x, y).item(1)), 8)
    return pearsons_r


def calculate_slope(x, y):
    # slope = round(
    #     np.polyfit(x.astype(float), y.astype(float), 1).item(0),
    #     8,
    # )
    slope = np.polyfit(x.astype(float), y.astype(float), 1).item(0)

    return slope
