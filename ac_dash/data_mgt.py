import logging
import pandas as pd
import pandas.api.types as ptypes
from datetime import timedelta
from sqlalchemy import (
    Table,
    update,
    text,
    delete,
    PrimaryKeyConstraint,
    inspect,
    distinct,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql import select, desc
from flask_sqlalchemy import SQLAlchemy
from .db import engine

db = SQLAlchemy()
logger = logging.getLogger("defaultLogger")


class Flux(db.Model):
    __tablename__ = "flux_table"
    start_time = db.Column(db.DateTime(timezone=True), index=True, primary_key=True)
    chamber_id = db.Column(db.String, primary_key=True)
    instrument_serial = db.Column(db.String(25), nullable=False, primary_key=True)
    instrument_model = db.Column(db.String(25), nullable=False)
    updated_height = db.Column(db.Boolean, default=False)
    # offsets from start_time in seconds
    CH4_offset_s = db.Column(db.Integer, nullable=True)
    CH4_offset_e = db.Column(db.Integer, nullable=True)
    CO2_offset_s = db.Column(db.Integer, nullable=True)
    CO2_offset_e = db.Column(db.Integer, nullable=True)
    N2O_offset_s = db.Column(db.Integer, nullable=True)
    N2O_offset_e = db.Column(db.Integer, nullable=True)
    close_offset = db.Column(db.Integer, nullable=True)
    open_offset = db.Column(db.Integer, nullable=True)
    end_offset = db.Column(db.Integer, nullable=True)

    air_pressure = db.Column(db.Float, nullable=True)
    air_temperature = db.Column(db.Float, nullable=True)
    chamber_height = db.Column(db.Float, nullable=True)
    lagtime = db.Column(db.Integer, nullable=True)
    quality_r = db.Column(db.Float, nullable=True)
    quality_r2 = db.Column(db.Float, nullable=True)

    CH4_slope = db.Column(db.Float, nullable=True)
    CH4_r = db.Column(db.Float, nullable=True)
    CH4_r2 = db.Column(db.Float, nullable=True)
    CH4_flux = db.Column(db.Float, nullable=True)
    CO2_slope = db.Column(db.Float, nullable=True)
    CO2_r = db.Column(db.Float, nullable=True)
    CO2_r2 = db.Column(db.Float, nullable=True)
    CO2_flux = db.Column(db.Float, nullable=True)
    N2O_slope = db.Column(db.Float, nullable=True)
    N2O_r = db.Column(db.Float, nullable=True)
    N2O_r2 = db.Column(db.Float, nullable=True)
    N2O_flux = db.Column(db.Float, nullable=True)
    is_valid = db.Column(db.Boolean)
    error_code = db.Column(db.Integer, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint(
            "start_time", "chamber_id", "instrument_serial", name="pk_start_id_serial"
        ),
    )

    def __repr__(self):
        return f"<Flux(id={self.id}, chamber_id={self.chamber_id}, lagtime={self.lagtime}, slope={self.slope}, r2={self.r2})>"


Flux_tbl = Table("flux_table", Flux.metadata)


def mk_flux_table():
    Flux.metadata.create_all(engine)


def add_flux(
    start_time,
    lagtime,
    chamber_id,
    CH4_offset_s,
    CH4_offset_e,
    CO2_offset_s,
    CO2_offset_e,
    CH4_slope,
    CH4_r,
    CH4_r2,
    CH4_flux,
    CO2_slope,
    CO2_r,
    CO2_r2,
    CO2_flux,
    is_valid,
):
    ins = Flux_tbl.insert().values(
        start_time=start_time,
        lagtime=lagtime,
        chamber_id=chamber_id,
        CH4_offset_s=CH4_offset_s,
        CH4_offset_e=CH4_offset_e,
        CO2_offset_s=CO2_offset_s,
        CO2_offset_e=CO2_offset_e,
        CH4_slope=CH4_slope,
        CH4_r=CH4_r,
        CH4_r2=CH4_r2,
        CH4_flux=CH4_flux,
        CO2_slope=CO2_slope,
        CO2_r=CO2_r,
        CO2_r2=CO2_r2,
        CO2_flux=CO2_flux,
        is_valid=is_valid,
    )
    print(f'Adding flux at "{start_time}".')

    with engine.begin() as conn:
        conn.execute(ins)


def flux_df_to_table(df, instrument_serial):
    table = Flux.__tablename__
    primary_keys = get_primary_keys(table, engine)
    logger.debug(f"Pushing {len(df)} rows to local db.")
    logger.debug(df)
    with engine.begin() as con:
        df_new, _ = drop_pk_dupes(df, table, primary_keys, con)
        df_new.to_sql("flux_table", con=con, if_exists="append", index=False)
    return df_new


def single_flux_to_table(df):
    logger.debug(f"Pushing {len(df)} rows to local db.")
    logger.debug(df.iloc[0])
    start_time = df["start_time"].iloc[0]
    chamber_id = df["chamber_id"].iloc[0]
    instrument_serial = df["instrument_serial"].iloc[0]
    to_delete = delete(Flux_tbl).where(
        Flux_tbl.c.start_time == start_time,
        Flux_tbl.c.chamber_id == chamber_id,
        Flux_tbl.c.instrument_serial == instrument_serial,
    )
    with engine.begin() as con:
        con.execute(to_delete)
        df.to_sql("flux_table", con=con, if_exists="append", index=False)


def fluxes_to_table(df):
    with engine.begin() as con:
        df.to_sql("flux_table", con=con, if_exists="append", index=False)


# def update_flux(measurement):
#     session = Session(engine)
#     stmt = (
#         update(Flux_tbl)
#         .where(
#             Flux_tbl.c.start_time == measurement.start_time,
#             Flux_tbl.c.instrument_serial == measurement.instrument.serial,
#             Flux_tbl.c.chamber_id == measurement.chamber_id,
#         )
#         .values({column: value})
#     )
#     session.execute(stmt)
#     session.commit()
#     session.close()


def flux_table_to_df(cols=None):
    # Query all records from the flux_table table
    if cols is not None:
        columns = [Flux_tbl.c[col] for col in cols]
        select_st = select(*columns).order_by(desc(Flux_tbl.c.start_time))
    else:
        select_st = select(
            Flux_tbl,
        ).order_by(desc(Flux_tbl.c.start_time))

    with engine.connect() as conn:
        df = pd.read_sql(select_st, conn)

    df.sort_values("start_time", inplace=True)
    return df


def flux_to_df_complete(start_time, serial, conn=None):
    select_st = select(Flux_tbl).where(
        Flux_tbl.c.start_time == start_time,
        # NOTE: Queries are relatively small anyway so how much sense is there
        # to filter the query filter with chamber_id?
        #     Flux_tbl.c.chamber_id == measurement.chamber_id,
        Flux_tbl.c.instrument_serial == serial,
    )
    volume_select_st = select(Volume_tbl).where(
        Volume_tbl.c.datetime >= (start_time - pd.Timedelta(days=365)),
        Volume_tbl.c.datetime <= start_time,
    )
    meteo_select_st = select(Meteo_tbl).where(
        Meteo_tbl.c.datetime >= (start_time - pd.Timedelta(days=1)),
        Meteo_tbl.c.datetime <= start_time,
    )
    if conn is not None:
        df = pd.read_sql(select_st, conn)
        vol_df = pd.read_sql(volume_select_st, conn)
        meteo_df = pd.read_sql(meteo_select_st, conn)
        if df.empty:
            return None
    else:
        with engine.connect() as conn:
            df = pd.read_sql(select_st, conn)
            vol_df = pd.read_sql(volume_select_st, conn)
            meteo_df = pd.read_sql(meteo_select_st, conn)
            if df.empty:
                return None

    df.sort_values("start_time")
    # TODO: Compare chamber height every time to see if it has updated?
    if not df["chamber_height"].iloc[0]:
        df.drop("chamber_height", axis=1, inplace=True)
        vol_df.drop("chamber_id", axis=1, inplace=True)
        if not vol_df.empty:
            vol_df.sort_values("datetime")
            df = pd.merge_asof(
                df,
                vol_df,
                left_on="start_time",
                right_on="datetime",
                direction="backward",
                tolerance=pd.Timedelta(days=365),
            )
    if not df["air_pressure"].iloc[0] and not df["air_pressure"].iloc[0]:
        df.drop("air_pressure", axis=1, inplace=True)
        df.drop("air_temperature", axis=1, inplace=True)
        if not meteo_df.empty:
            vol_df.sort_values("datetime")
            df = pd.merge_asof(
                df,
                vol_df,
                left_on="start_time",
                right_on="datetime",
                direction="nearest",
                tolerance=pd.Timedelta(minutes=30),
            )
    return df


def flux_to_df(start_time, serial, conn=None):
    select_st = select(Flux_tbl).where(
        Flux_tbl.c.start_time == start_time,
        # NOTE: Queries are relatively small anyway so how much sense is there
        # to filter the query filter with chamber_id?
        #     Flux_tbl.c.chamber_id == measurement.chamber_id,
        Flux_tbl.c.instrument_serial == serial,
    )
    if conn is not None:
        df = pd.read_sql(select_st, conn)
    else:
        with engine.connect() as conn:
            df = pd.read_sql(select_st, conn)

    df.sort_values("start_time")
    return df


def delete_fluxes(start, end):
    delete_st = delete(Flux_tbl).where(
        Flux_tbl.c.start_time > start, Flux_tbl.c.start_time < end
    )
    with engine.connect() as conn:
        conn.execute(delete_st)


def flux_range_to_df(start, end, chamber_ids, is_valid=None, serial=None):
    logger.info(start)
    logger.info(end)
    select_st = select(Flux_tbl).where(
        Flux_tbl.c.start_time >= start,
        Flux_tbl.c.start_time <= end,
        Flux_tbl.c.chamber_id.in_(chamber_ids),
    )
    if serial is not None:
        select_st = select_st.where(Flux_tbl.c.instrument_serial == serial)
    if is_valid is not None:
        select_st = select_st.where(Flux_tbl.c.is_valid == is_valid)
    select_st = select_st.order_by(Flux_tbl.c.start_time.asc())
    with engine.connect() as conn:
        df = pd.read_sql(select_st, conn)

    return df


class GasMeasurement(db.Model):
    __tablename__ = "gas_table"
    instrument_model = db.Column(db.String(25), nullable=False, index=True)
    instrument_serial = db.Column(db.String(25), nullable=False, index=True)
    datetime = db.Column(db.DateTime(timezone=True), index=True)
    CH4 = db.Column(db.Float, nullable=True)
    CO2 = db.Column(db.Float, nullable=True)
    N2O = db.Column(db.Float, nullable=True)
    H2O = db.Column(db.Float, nullable=True)
    DIAG = db.Column(db.Float, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint(
            "datetime", "instrument_serial", name="pk_datetime_serial"
        ),
    )


Gas_tbl = Table("gas_table", GasMeasurement.metadata)


def mk_gas_table():
    GasMeasurement.metadata.create_all(engine)


# NOTE: move this down
def gas_table_to_df(start=None, end=None, serial=None, conn=None):
    if start is None:
        start = pd.to_datetime("1970-01-01", format="ISO8601")
    if end is None:
        end = pd.to_datetime("2040-01-01", format="ISO8601")
    select_st = select(Gas_tbl).where(
        Gas_tbl.c.datetime >= start, Gas_tbl.c.datetime <= end
    )
    if serial is not None:
        select_st = select_st.where(Gas_tbl.c.instrument_serial == serial)
    if conn:
        df = pd.read_sql(select_st, conn)
        df.set_index("datetime", inplace=True)
    else:
        with engine.connect() as conn:
            df = pd.read_sql(select_st, conn)
            df.set_index("datetime", inplace=True)

    return df


def get_distinct_instrument():
    query = query = select(Gas_tbl.c.instrument_serial).distinct()

    # Execute the query
    with engine.connect() as connection:
        result = connection.execute(query)
        distinct_values = [row[0] for row in result]
    return distinct_values


def df_to_gas_table(df):
    table_name = GasMeasurement.__tablename__
    primary_keys = get_primary_keys(table_name, engine)
    df.drop_duplicates(subset=primary_keys, keep="first", inplace=True)
    logger.debug(f"Pushing {len(df)} rows to local db.")
    with engine.begin() as con:
        df_copy, dupes = drop_pk_dupes(df, table_name, primary_keys, con)
        if not df_copy.empty:
            logger.debug(f"Pushing {len(df_copy)} to local DB")
            df_copy.to_sql("gas_table", con=con, if_exists="append", index=False)
    return df_copy, dupes


class Cycles(db.Model):
    __tablename__ = "cycle_table"
    start_time = db.Column(db.DateTime(timezone=True), primary_key=True, index=True)
    close_offset = db.Column(db.Integer, index=True, nullable=False)
    open_offset = db.Column(db.Integer, index=True, nullable=False)
    end_offset = db.Column(db.Integer, index=True, nullable=False)
    chamber_id = db.Column(db.String, index=True, nullable=False)


Cycle_tbl = Table("cycle_table", Cycles.metadata)


def mk_cycle_table():
    Cycles.metadata.create_all(engine)


def df_to_cycle_table(df):
    """
    Push dataframe into the cycles table, dataframe doesnt need to have all of
    the columns of the table.
    Only pushes cycle if start_time doesnt exist in table.

    Parameters
    ----------
    df : dataframe


    """
    table = Cycles.__tablename__
    primary_keys = get_primary_keys(table, engine)
    with engine.begin() as conn:
        df_new, _ = drop_pk_dupes(df, table, primary_keys, conn)
        df_new.to_sql("cycle_table", con=conn, if_exists="append", index=False)
        return df_new


def cycle_table_to_df(start, end, conn=None):
    select_st = select(Cycle_tbl).where(
        Cycle_tbl.c.start_time >= start, Cycle_tbl.c.start_time <= end
    )
    if conn:
        df = pd.read_sql(select_st, conn)
        logger.debug(df)
        df.sort_values(by="start_time", inplace=True)
    else:
        with engine.connect() as conn:
            df = pd.read_sql(select_st, conn)
            df.sort_values(by="start_time", inplace=True)

    return df


class Meteo(db.Model):
    __tablename__ = "meteo_table"
    datetime = db.Column(db.DateTime(timezone=True), primary_key=True, index=True)
    source = db.Column(db.String, primary_key=True)
    air_temperature = db.Column(db.Float, index=True)
    air_pressure = db.Column(db.Float, index=True)

    __table_args__ = (
        PrimaryKeyConstraint("datetime", "source", name="pk_datetime_source"),
    )


Meteo_tbl = Table("meteo_table", Meteo.metadata)


def get_distinct_meteo_source():
    query = query = select(Meteo_tbl.c.source).distinct()

    # Execute the query
    with engine.connect() as connection:
        result = connection.execute(query)
        distinct_values = [row[0] for row in result]
    return distinct_values


def get_single_meteo(timestamp, source=None):
    logger.debug("Running get single temp")
    # select_st = select(Meteo_tbl).where(
    #     Meteo_tbl.c.datetime >= (start - pd.Timedelta(days=1)),
    #     Meteo_tbl.c.datetime <= (start + pd.Timedelta(days=1)),
    # )
    start = timestamp - pd.Timedelta(minutes=30)
    end = timestamp + pd.Timedelta(minutes=30)
    # query = f"""SELECT *
    #         FROM meteo_table
    #         ORDER BY ABS(EXTRACT(EPOCH FROM (your_datetime_column - TIMESTAMP '{start}')))
    #         LIMIT 1;"""
    query = f"""
            SELECT *
            FROM meteo_table
            WHERE datetime BETWEEN TIMESTAMP '{start}' AND TIMESTAMP '{end}'"""

    if source is not None:
        query += f" AND source = '{source}'"

    # Add the ORDER BY and LIMIT clauses
    query += f"""
        ORDER BY ABS(EXTRACT(EPOCH FROM (datetime - TIMESTAMP '{start}')))
        LIMIT 1;
    """
    df = pd.read_sql_query(query, engine)
    if df is None or df.empty:
        return None, None
    data = df.iloc[0]

    air_temperature = data.get("air_temperature")
    air_pressure = data.get("air_pressure")

    return air_temperature, air_pressure


def apply_meteo_table_trigger():
    """
    Function that runs every time something changes in meteo_table.
    Updates the air_temperature and air_pressure values in flux_table
    based on the nearest start_time values.
    """
    trigger_func_name = "update_meteo_trigger"
    trigger_name = "meteo_update_trigger"
    trigger_function_sql = f"""
    CREATE OR REPLACE FUNCTION {trigger_func_name}()
    RETURNS TRIGGER AS $$
    DECLARE
        nearest_start_time TIMESTAMP; -- Variable to hold the nearest start_time
    BEGIN
        -- Find the nearest start_time in flux_table
        SELECT start_time
        INTO nearest_start_time
        FROM flux_table
        WHERE ABS(EXTRACT(EPOCH FROM (start_time - NEW.datetime))) = (
            SELECT MIN(ABS(EXTRACT(EPOCH FROM (start_time - NEW.datetime))))
            FROM flux_table
        )
        LIMIT 1;

        -- Update flux_table with the air_temperature and air_pressure values
        UPDATE flux_table
        SET updated_meteo = TRUE,
        SET air_temperature = NEW.air_temperature,
            air_pressure = NEW.air_pressure
        WHERE start_time = nearest_start_time;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """

    trigger_sql = f"""
    CREATE TRIGGER {trigger_name}
    AFTER INSERT OR UPDATE OR DELETE ON meteo_table
    FOR EACH ROW
    EXECUTE FUNCTION {trigger_func_name}();
    """

    # Execute the SQL statements
    with engine.begin() as conn:
        try:
            conn.execute(text(trigger_function_sql))
            conn.execute(text(trigger_sql))
        except Exception as e:
            print(e)
            pass


def mk_meteo_table():
    Meteo.metadata.create_all(engine)


def df_to_meteo_table(df):
    logger.debug(df)
    table = Meteo.__tablename__
    primary_keys = get_primary_keys(table, engine)
    print(table)
    print(primary_keys)
    with engine.begin() as con:
        df_new, _ = drop_pk_dupes(df, table, primary_keys, con)
        logger.debug(df_new)
        logger.debug(table)
        df_new.to_sql(table, con=con, if_exists="append", index=False)

    return df_new


def meteo_table_to_df(start=None, end=None):
    if start is None:
        start = pd.to_datetime("1970-01-01", format="ISO8601")
    if end is None:
        end = pd.to_datetime("2040-01-01", format="ISO8601")
    start = start - timedelta(hours=2)
    end = end + timedelta(hours=2)
    select_st = select(Meteo_tbl).where(
        Meteo_tbl.c.datetime >= start, Meteo_tbl.c.datetime <= end
    )
    with engine.connect() as conn:
        df = pd.read_sql(select_st, conn)
        df.sort_values(by="datetime", inplace=True)

    return df


# TODO: Create this table programmatically from json
class Volume(db.Model):
    __tablename__ = "volume_table"
    datetime = db.Column(db.DateTime(timezone=True), primary_key=True, index=True)
    chamber_id = db.Column(db.String, primary_key=True)
    nw = db.Column(db.Float)
    sw = db.Column(db.Float)
    se = db.Column(db.Float)
    ne = db.Column(db.Float)
    mid = db.Column(db.Float)
    has_snow = db.Column(db.Integer)
    chamber_height = db.Column(db.Float, nullable=True)
    unit = db.Column(db.String)

    # must be a tuple even with just one item
    __table_args__ = (
        PrimaryKeyConstraint("datetime", "chamber_id", name="pk_datetime_id"),
    )


Volume_tbl = Table("volume_table", Volume.metadata)


def mk_volume_table():
    Volume.metadata.create_all(engine)


def get_single_volume(timestamp):
    logger.debug("Running get single temp")
    # select_st = select(Meteo_tbl).where(
    #     Meteo_tbl.c.datetime >= (start - pd.Timedelta(days=1)),
    #     Meteo_tbl.c.datetime <= (start + pd.Timedelta(days=1)),
    # )
    start = timestamp - pd.Timedelta(days=365)
    end = timestamp + pd.Timedelta(minutes=365)
    # query = f"""SELECT *
    #         FROM meteo_table
    #         ORDER BY ABS(EXTRACT(EPOCH FROM (your_datetime_column - TIMESTAMP '{start}')))
    #         LIMIT 1;"""
    query = f"""
            SELECT *
            FROM meteo_table
            WHERE datetime BETWEEN TIMESTAMP '{start}' 
                                        AND TIMESTAMP '{end}'
            ORDER BY ABS(EXTRACT(EPOCH FROM (datetime - TIMESTAMP '{start}')))
            LIMIT 1;
            """
    df = pd.read_sql_query(query, engine)
    if df is None or df.empty:
        return None, None
    data = df.iloc[0]

    height = data.get("height")

    return height


def df_to_volume_table(df):
    table = Volume.__tablename__
    primary_keys = get_primary_keys(table, engine)
    with engine.begin() as con:
        df_new, _ = drop_pk_dupes(df, table, primary_keys, con)
        df_new.to_sql(table, con=con, if_exists="append", index=False)
    return df_new


def check_if_exists(engine, object_name, object_type):
    """
    Checks if a database object (function or trigger) exists.

    Args:
        engine: SQLAlchemy engine connection.
        object_name: Name of the object to check.
        object_type: Type of the object ('function' or 'trigger').

    Returns:
        True if the object exists, False otherwise.
    """
    if object_type == "function":
        query = f"""
        SELECT 1
        FROM pg_proc
        WHERE proname = '{object_name}';
        """
    elif object_type == "trigger":
        query = f"""
        SELECT 1
        FROM pg_trigger
        WHERE tgname = '{object_name}';
        """
    else:
        raise ValueError("Invalid object type. Use 'function' or 'trigger'.")

    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        return result is not None


def apply_volume_table_trigger():
    """
    Function that runs every time something changes in volume_table.
    Changes the chamber_height and updated_height (to true) for relevant measurements in
    flux_table.
    """
    trigger_name = "height_update_trigger"
    trigger_func_name = "update_volume_trigger"
    trigger_function_sql = f"""
    CREATE OR REPLACE FUNCTION {trigger_func_name}()
    RETURNS TRIGGER AS $$
    DECLARE
        next_datetime TIMESTAMP; -- Variable to hold the next newer datetime
    BEGIN
        -- Find the next newer datetime in volume_table
        SELECT MIN(datetime)
        INTO next_datetime
        FROM volume_table
        WHERE chamber_id = NEW.chamber_id
        AND datetime > NEW.datetime;

        -- Update flux_table only for rows within the specified range
        UPDATE flux_table
        SET updated_height = TRUE,
        chamber_height = NEW.chamber_height
        WHERE chamber_id = NEW.chamber_id
        AND start_time >= NEW.datetime
        AND start_time < next_datetime;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """

    trigger_sql = f"""
    CREATE TRIGGER {trigger_name}
    AFTER UPDATE OR INSERT OR DELETE ON volume_table
    FOR EACH ROW
    EXECUTE FUNCTION {trigger_func_name}();
    """

    with engine.begin() as conn:
        try:
            if not check_if_exists(engine, trigger_func_name, "function"):
                conn.execute(text(trigger_function_sql))
            if not check_if_exists(engine, trigger_name, "trigger"):
                conn.execute(text(trigger_sql))
        except Exception as e:
            print(e)
            pass


def volume_table_to_df(start=None, end=None):
    if start is None:
        start = pd.to_datetime("1970-01-01", format="ISO8601")
    if end is None:
        end = pd.to_datetime("2040-01-01", format="ISO8601")
    start = start - timedelta(days=365)
    end = end + timedelta(days=365)
    select_st = select(Volume_tbl).where(
        Volume_tbl.c.datetime >= start, Volume_tbl.c.datetime <= end
    )
    with engine.connect() as conn:
        df = pd.read_sql(select_st, conn)
        df.sort_values(by="datetime", inplace=True)

    return df


def del_volume_measurement(time, id):
    to_delete = delete(Volume_tbl).where(
        Volume_tbl.c.datetime == time, Volume_tbl.c.chamber_id == id
    )
    with engine.begin() as con:
        con.execute(to_delete)


def get_primary_keys(table_name, engine):
    """
    Retrieve the primary key columns of a SQL table.

    Parameters:
        table_name (str): The name of the table.
        engine: The SQLAlchemy engine connected to the database.

    Returns:
        list: A list of column names that are primary keys.
    """
    inspector = inspect(engine)
    primary_keys = inspector.get_pk_constraint(table_name).get(
        "constrained_columns", None
    )
    if not primary_keys:
        raise ValueError(f"Table '{table_name}' does not have a primary key.")
    return primary_keys


def drop_pk_dupes(df, table, primary_keys, con):
    """Filter dataframe using primary keys from db table"""
    # Drop duplicate rows based on primary keys in the dataframe
    df.drop_duplicates(subset=primary_keys, keep="first", inplace=True)

    # Identify datetime columns to define the range for SQL query
    datetime_cols = [
        col for col in df.columns if ptypes.is_datetime64_any_dtype(df[col])
    ]
    if datetime_cols:
        # Use the first datetime column to determine the range
        dt_col = datetime_cols[0]
        df.sort_values(dt_col, inplace=True)
        start = df[dt_col].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        end = df[dt_col].iloc[-1].strftime("%Y-%m-%d %H:%M:%S")
    else:
        raise ValueError("No datetime columns found in the dataframe.")

    # Construct SQL query to fetch existing rows in the datetime range
    key_columns = ", ".join(primary_keys)
    if start == end:
        query = f"SELECT {key_columns} FROM {table}"
    else:
        query = f"SELECT {key_columns} FROM {table} WHERE {dt_col} >= '{start}' AND {dt_col} <= '{end}'"
    existing_df = pd.read_sql(query, con)

    # If no existing rows are found, return the input dataframe and an empty duplicates dataframe
    if existing_df.empty:
        return df.copy(), pd.DataFrame(columns=df.columns)

    # Create composite keys for detecting duplicates
    df["composite_key"] = list(zip(*[df[key] for key in primary_keys]))
    existing_df["composite_key"] = list(
        zip(*[existing_df[key] for key in primary_keys])
    )

    # Identify duplicates
    existing_keys = set(existing_df["composite_key"])
    duplicates = df[df["composite_key"].isin(existing_keys)].copy()

    # Filter out duplicates from the original dataframe
    df_filtered = df[~df["composite_key"].isin(existing_keys)].copy()

    # Drop the temporary composite_key column
    df_filtered.drop(columns=["composite_key"], inplace=True)
    duplicates.drop(columns=["composite_key"], inplace=True)

    return df_filtered, duplicates
