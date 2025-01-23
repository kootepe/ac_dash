import io
import base64

from .measuring import instruments
from .data_mgt import df_to_gas_table
from .utils import process_measurement_file, process_measurement_zip


def read_gas_init_input(use_class, serial, model, contents, filename):
    """Read data passed from the settings page"""
    # global instruments
    if serial is None or model is None:
        return "Select instrument or fill in instrument details", ""
    content_type, content_str = contents.split(",")
    ext = filename.split(".")[-1].lower()
    decoded = base64.b64decode(content_str)
    instrument = instruments.get(use_class)(serial)
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
            push_rows, in_rows = process_measurement_zip(
                io.BytesIO(decoded), instrument
            )
            return "", f"Pushed {push_rows}/{in_rows} rows."
        else:
            return "Wrong filetype extension", ""
    except Exception as e:
        return f"Exception {e}", ""
