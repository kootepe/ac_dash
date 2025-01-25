from abc import ABC, abstractmethod
import pandas as pd


class Instrument(ABC):
    """
    Abstract base class for instruments.
    Defines the required methods and attributes for any instrument.
    """

    def __init__(self, model, serial):
        self.model = model
        self.serial = serial

    @property
    @abstractmethod
    def gases(self):
        """List of gases measured by the instrument."""
        pass

    @property
    @abstractmethod
    def flux_gases(self):
        """List of gases measured by the instrument."""
        pass

    @property
    @abstractmethod
    def units(self):
        """Units for the gases measured."""
        pass

    @abstractmethod
    def read_output_file(self, file_path):
        """Function to read the instrument's output file."""
        pass

    @abstractmethod
    def __repr__(self):
        pass


class LI7810(Instrument):
    """
    Implementation for LI-COR LI-7810 gas analyzer.

    Attributes
    ----------
    gases : list
    List of measured gases

    units : dict
    Dictionary of measured gases and their units

    """

    def __init__(self, serial):
        # Ensure the base class attributes are initialized
        model = "LI-7810"
        super().__init__(model, serial)
        self._gases = ["CO2", "CH4", "H2O"]
        self._flux_gases = ["CO2", "CH4"]
        self._units = {"CO2": "ppm", "CH4": "ppb", "H2O": "ppm"}
        self.pd_kwargs = {
            "skiprows": [0, 1, 2, 3, 4, 6],
            "sep": "\t",
            "usecols": ["DATE", "TIME", "DIAG", "H2O", "CO2", "CH4"],
            "dtype": {
                "CH4": "float",
                "CO2": "float",
                "H2O": "float",
                "DATE": "str",
                "TIME": "str",
                "DIAG": "int",
            },
            # "index_col": "datetime",
            # pandas will combine these columns and parse the dates with
            # date_fromat
            "parse_dates": {"datetime": ["DATE", "TIME"]},
            "date_format": "%Y-%m-%d %H:%M:%S",
        }
        self.diag_col = "DIAG"

    @property
    def gases(self):
        return self._gases

    @property
    def flux_gases(self):
        return self._flux_gases

    @property
    def units(self):
        return self._units

    def read_output_file(self, file_path):
        return pd.read_csv(file_path, **self.pd_kwargs)

    def __repr__(self):
        return f"{self.model}, {self.serial}"


class LI7810_reduced(Instrument):
    """
    Implementation for LI-COR LI-7810 gas analyzer and the reduced output file used at oulanka.

    Attributes
    ----------
    gases : list
    List of measured gases

    units : dict
    Dictionary of measured gases and their units

    """

    def __init__(self, serial):
        # Ensure the base class attributes are initialized
        model = "LI-7810"
        super().__init__(model, serial)
        self._gases = ["CO2", "CH4", "H2O"]
        self._flux_gases = ["CO2", "CH4"]
        self._units = {"CO2": "ppm", "CH4": "ppb", "H2O": "ppm"}
        self.pd_kwargs = {
            "sep": ",",
            "dtype": {
                "CH4": "float",
                "CO2": "float",
                "H2O": "float",
                "DIAG": "int",
                "datetime": "str",
            },
            # "index_col": "datetime",
            # pandas will combine these columns and parse the dates with
            # date_fromat
            "parse_dates": ["datetime"],
            "date_format": "%Y-%m-%d %H:%M:%S",
        }
        self.diag_col = "DIAG"

    @property
    def gases(self):
        return self._gases

    @property
    def flux_gases(self):
        return self._flux_gases

    @property
    def units(self):
        return self._units

    def read_output_file(self, file_path):
        return pd.read_csv(file_path, **self.pd_kwargs)

    def __repr__(self):
        return f"{self.model}, {self.serial}"


class LI7820(Instrument):
    """
    Implementation for LI-COR LI-7820 gas analyzer.

    Attributes
    ----------
    gases : list
    List of measured gases

    units : dict
    Dictionary of measured gases and their units

    """

    def __init__(self, serial):
        model = "LI-7820"
        super().__init__(model, serial)
        self._gases = ["N2O", "H2O"]
        self._flux_gases = ["N2O"]
        self._units = {"N2O": "ppb", "H2O": "ppm"}
        self.pd_kwargs = {
            "skiprows": [0, 1, 2, 3, 4, 6],
            "sep": "\t",
            "usecols": ["DATE", "TIME", "DIAG", "H2O", "N2O"],
            "dtype": {
                "N2O": "float",
                "H2O": "float",
                "DATE": "str",
                "TIME": "str",
                "DIAG": "int",
            },
            # "index_col": "datetime",
            # pandas will combine these columns and parse the dates with
            # date_fromat
            "parse_dates": {"datetime": ["DATE", "TIME"]},
            "date_format": "%Y-%m-%d %H:%M:%S",
        }
        self.diag_col = "DIAG"

    @property
    def gases(self):
        return self._gases

    @property
    def flux_gases(self):
        return self._flux_gases

    @property
    def units(self):
        return self._units

    def read_output_file(self, file_path):
        return pd.read_csv(file_path, **self.pd_kwargs)

    def __repr__(self):
        return f"{self.model}, {self.serial}"


class LI7820_reduced(Instrument):
    """
    Implementation for LI-COR LI-7820 gas analyzer and the reduced output file used at oulanka.

    Attributes
    ----------
    gases : list
    List of measured gases

    units : dict
    Dictionary of measured gases and their units

    """

    def __init__(self, serial):
        # Ensure the base class attributes are initialized
        model = "LI-7820"
        super().__init__(model, serial)
        self._gases = ["N2O", "H2O"]
        self._flux_gases = ["N2O"]
        self._units = {"N2O": "ppb", "H2O": "ppm"}
        self.pd_kwargs = {
            "sep": ",",
            "dtype": {
                "datetime": "str",
                "DIAG": "int",
                "H2O": "float",
                "N2O": "float",
            },
            # "index_col": "datetime",
            # pandas will combine these columns and parse the dates with
            # date_fromat
            # "parse_dates": ["datetime"],
            # "date_format": "ISO8601",
            # "date_format": "%Y-%m-%d %H:%M:%S",
        }
        self.diag_col = "DIAG"

    @property
    def gases(self):
        return self._gases

    @property
    def flux_gases(self):
        return self._flux_gases

    @property
    def units(self):
        return self._units

    def read_output_file(self, file_path):
        df = pd.read_csv(file_path, **self.pd_kwargs)
        print(df)
        df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")
        return df

    def __repr__(self):
        return f"{self.model}, {self.serial}"


instruments = {
    "LI7810": LI7810,
    "LI7810_reduced": LI7810_reduced,
    "LI7820": LI7820,
    "LI7820_reduced": LI7820_reduced,
}
class_model_key = {
    "LI7810": "LI-7810",
    "LI7810_reduced": "LI-7810",
    "LI7820": "LI-7820",
    "LI7820_reduced": "LI-7820",
}
