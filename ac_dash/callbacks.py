import logging
import traceback
import io
import json
import pandas as pd
import requests
import base64
from dash import (
    dcc,
    Output,
    dash_table,
    Input,
    State,
    ctx,
    callback_context,
    no_update,
    html,
    ALL,
)

from flask import request as flask_request, request
from .db import engine

from .utils import (
    handle_triggers,
    no_data_response,
    load_measurement_data,
    execute_actions,
    create_gas_plots,
    create_attribute_graph,
    generate_measurement_info,
    parse_date_range,
    process_measurement_zip,
    process_measurement_file,
    process_protocol_file,
    process_protocol_zip,
    init_from_cycle_table,
)
from .api.routes import CycleApi
from .measuring import instruments
from .data_mgt import (
    flux_range_to_df,
    cycle_table_to_df,
    flux_table_to_df,
    volume_table_to_df,
    meteo_table_to_df,
    gas_table_to_df,
    df_to_gas_table,
    df_to_cycle_table,
    df_to_meteo_table,
)
from .create_graph import apply_graph_zoom
from .layout import (
    mk_settings,
    mk_settings_page,
    mk_main_page,
    create_layout,
    graph_style,
)
from .db_view_page import mk_db_view_page
from .data_mgt import Cycle_tbl

cycle_tbl_cols = [col.name for col in Cycle_tbl.columns]

logger = logging.getLogger("defaultLogger")


def mk_binds(settings):
    binds = []
    for key, elem in settings.items():
        bind = f"""if (event.key == '{key}') {{
                document.getElementById('{elem}').click()
            }}\n"""
        binds.append(bind)
    binds_js = "".join(binds)
    keybinds = f"""
    function(id) {{
        document.addEventListener("keydown", function(event) {{
            {binds_js}
        }});
        return window.dash_clientside.no_update;
    }}
    """
    return keybinds


def register_callbacks(
    app,
    url,
    main_page,
    settings_page,
    ifdb_read_dict,
    ifdb_push_dict,
    chambers,
    chamber_map,
    graph_names,
    settings,
):
    # bind h and l to previous and next buttons
    app.clientside_callback(
        mk_binds(settings["settings"]["keybinds"]),
        *[Output(elem, "id") for key, elem in settings["settings"]["keybinds"].items()],
        # [[State(graph_name, "relayoutData") for graph_name in graph_names[1]]],
        # Output("prev-button", "id"),
        # Output("next-button", "id"),
        *[Input(elem, "id") for key, elem in settings["settings"]["keybinds"].items()],
        # Input("prev-button", "id"),
        # Input("next-button", "id"),
    )

    @app.callback(
        Output("model-input-div", "style"),
        Output("model-input", "disabled"),
        Output("model-input", "value"),
        Output("serial-input-div", "style"),
        Output("serial-input", "disabled"),
        Output("serial-input", "value"),
        Output("class-input-div", "style"),
        Output("class-input", "disabled"),
        Output("class-input", "value"),
        Input("instrument-select", "value"),
        State("serial-input", "style"),
        State("settings-store", "data"),
        prevent_initial_call=True,
    )
    def show_serial_input(value, serial_style, stored):
        """
        Show instrument details based on the selected instrument.

        Parameters
        ----------
        value : dictionary
            The options in the dropdown map to a dictionary defined in the
            initial layout.

        serial_style : dictionary

        stored : dictionary



        Returns
        -------









        """
        # NOTE: This is a mess that needs to be sorted.
        if value is None:
            return (
                serial_style,
                False,
                "",
                serial_style,
                False,
                "",
                serial_style,
                False,
                "",
            )
        # NOTE: value is a dictionary:
        # {arbitrary instrument name: {
        # model: instrument_model,
        # class : class representation of instrument in measuring.py}}
        value = json.loads(value)

        if value is None:
            serial_style["display"] = "none"
            return (
                serial_style,
                False,
                "",
                serial_style,
                False,
                "",
                serial_style,
                False,
                "",
            )
        selected = next(
            (v for v in value.keys()),
            None,
        )
        model = next(
            (
                v["model"]
                for v in value.values()
                if isinstance(v, dict) and "model" in v
            ),
            None,
        )
        use_class = next(
            (
                v["class"]
                for v in value.values()
                if isinstance(v, dict) and "class" in v
            ),
            None,
        )
        # names appended with Generic are the available instruments defined in measuring.py
        if "Generic" in selected:
            serial_style["display"] = "block"
            return (
                serial_style,
                True,
                model,
                serial_style,
                False,
                "",
                no_update,
                False,
                use_class,
            )
        else:
            serial_style["display"] = "block"
            for key, item in value.items():
                serial = next(
                    (
                        v["serial"]
                        for v in value.values()
                        if isinstance(v, dict) and "serial" in v
                    ),
                    None,
                )
                # serial = item["serial"]
                # model = item["model"]
            return (
                serial_style,
                True,
                model,
                serial_style,
                True,
                serial,
                no_update,
                False,
                use_class,
            )

    @app.callback(
        Output("init-flux-warn", "children"),
        Input("init-flux", "n_clicks"),
        State("init-start", "value"),
        State("init-end", "value"),
        prevent_initial_call=True,
    )
    def init_flux(init, start, end):
        logger.info("Initiating")
        if start is None:
            return "Give start date"
        if end is None:
            return "Give end date"
        try:
            pd.to_datetime(start, format="%Y-%m-%d")
            pd.to_datetime(end, format="%Y-%m-%d")
        except Exception:
            return "give YYYY-MM-DD date"
        fluxes = flux_table_to_df()
        dupes = set(fluxes["start_time"])
        logger.debug(fluxes)
        if ctx.triggered_id == "init-flux":
            with engine.connect() as conn:
                df = cycle_table_to_df(start, end)
                if df.empty or df is None:
                    return f"No cycles between {start} and {end}."
                logger.debug(df)
                df = df[~df["start_time"].isin(dupes)]
                df.sort_values("start_time", inplace=True)
                logger.debug(df)
                init_from_cycle_table(df, None, None, conn)

    @app.callback(
        Output("model-input-warn", "children"),
        Output("model-input-show", "children"),
        State("class-input", "value"),
        State("serial-input", "value"),
        State("model-input", "value"),
        Input("upload-data", "contents"),
        State("upload-data", "filename"),
        prevent_initial_call=True,
    )
    def read_gas_init_input(use_class, model, serial, contents, filename):
        """Read data passed from the settings page"""
        # global instruments
        print(serial)
        print(model)
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
                df = process_measurement_zip(io.BytesIO(decoded), instrument)
                in_rows = len(df)
                df["datetime"] = (
                    df["datetime"]
                    .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                    .dt.tz_convert("UTC")
                )
                pushed_data, dupes = df_to_gas_table(df)
                push_rows = len(pushed_data)
                return "", f"Pushed {push_rows}/{in_rows}"
            else:
                return "Wrong filetype extension", ""
        except Exception as e:
            return f"Exception {e}", ""

    @app.callback(
        Output("protocol-input-warn", "children"),
        Output("protocol-input-show", "children"),
        Input("upload-protocol", "contents"),
        State("upload-protocol", "filename"),
        prevent_initial_call=True,
    )
    def read_cycle_init_input(contents, filename):
        """Read data passed from the settings page"""
        # global instruments
        content_type, content_str = contents.split(",")
        decoded = base64.b64decode(content_str)
        try:
            if "csv" in filename:
                df = pd.read_csv(io.StringIO(decoded.decode("utf-8")))
                df["start_time"] = pd.to_datetime(df["start_time"], format="ISO8601")
                try:
                    df["start_time"] = (
                        df["start_time"]
                        .dt.tz_localize("Europe/Helsinki", ambiguous=True)
                        .dt.tz_convert("UTC")
                    )
                except Exception:
                    pass
                inrows = len(df)
                pushed_data = df_to_cycle_table(df)

                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)
                return "", f"Pushed {inrows}/{row_count}"

            # Read the CSV into a Pandas DataFrame
            if "log" in filename:
                df = process_protocol_file(
                    io.StringIO(decoded.decode("utf-8")), chamber_map
                )
                in_cycles = len(df)

                pushed_data = df_to_cycle_table(df)
                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)
                return "", f"Pushed {row_count}/{in_cycles}"

            if "zip" in filename:
                df = process_protocol_zip(io.BytesIO(decoded), chamber_map)
                in_cycles = len(df)
                pushed_data = df_to_cycle_table(df)
                if pushed_data.empty:
                    row_count = 0
                else:
                    row_count = len(pushed_data)
                return "", f"Pushed {row_count}/{in_cycles}"
        except Exception as e:
            return f"Returned exception {e}", ""

    @app.callback(
        Output("meteo-input-warn", "children"),
        Output("meteo-input-show", "children"),
        State("meteo-source-input", "value"),
        Input("upload-meteo", "contents"),
        State("upload-meteo", "filename"),
        prevent_initial_call=True,
    )
    def read_meteo_init_input(source, contents, filename):
        """Read data passed from the settings page"""
        # global instruments
        if source is None:
            return "Select site.", ""
        content_type, content_str = contents.split(",")
        ext = filename.split(".")[-1].lower()
        decoded = base64.b64decode(content_str)
        file_exts = ["csv", "data", "dat"]

        def read_meteo_file(file):
            df = pd.read_csv(file)
            df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")
            df["datetime"] = (
                df["datetime"]
                .dt.tz_localize(
                    "Europe/Helsinki", ambiguous=True, nonexistent="shift_forward"
                )
                .dt.tz_convert("UTC")
            )
            return df

        try:
            file_exts = ["csv"]
            if ext in file_exts:
                logger.debug("Read file.")
                df = read_meteo_file(io.StringIO(decoded.decode("utf-8")))
                logger.debug("Adding source.")
                df["source"] = source
                in_rows = len(df)
                logger.debug("Pushing to table")
                pushed_data = df_to_meteo_table(df)
                push_rows = len(pushed_data)
                return "", f"Pushed {push_rows}/{in_rows}"

            else:
                return "Wrong filetype extension", ""
        except Exception as e:
            logger.debug(traceback.format_exc())
            return f"Exception {e}", ""

    @app.callback(Output("graph-div", "children"), Input("settings-store", "data"))
    def mk_display_graphs(settings):
        logger.debug(settings)
        left_graphs = settings["graph_names"][0]
        right_graphs = settings["graph_names"][1]
        lefts = [
            dcc.Graph(
                id={"type": "gas-graph", "index": graph_name},
                style=graph_style,
                config={"edits": {"shapePosition": True}},
            )
            for graph_name in left_graphs
        ]
        logger.debug(lefts)
        rights = [
            dcc.Graph(
                id={"type": "attrib-graph", "index": graph_name}, style=graph_style
            )
            for graph_name in right_graphs
        ]
        logger.debug(lefts)
        logger.debug(rights)

        # Dynamically create graphs based on graph_names
        return [
            html.Div(lefts),
            html.Div(rights),
        ]

    @app.callback(
        Output("settings-store", "data"),
        Output("show-settings", "children"),
        Input("save-settings", "n_clicks"),
        Input("settings-store", "data"),
        State("show-settings", "children"),
        prevent_initial_call=True,
    )
    def saved_settings(n_clicks, store, selected):
        logger.debug("Initial:")
        logger.debug(store)
        logger.debug(n_clicks)
        if n_clicks:
            for setting_part in selected:
                for settings in setting_part["props"]["children"]:
                    logger.debug("to be parsed:")
                    logger.debug(selected)
                    logger.debug(store)
                    if settings["props"].get("value", None) is not None:
                        setting = settings["props"]["id"]
                        value = settings["props"]["value"]
                        # show_type = settings["props"]["type"]
                        if setting == "gas_graphs":
                            names = [f"{gas}-plot" for gas in value]
                            store["graph_names"][0] = names
                        if setting == "attribute_graphs":
                            names = [f"{attrib}-graph" for attrib in value]
                            store["graph_names"][1] = names
                        # logger.debug(show_type)
                        logger.debug(store)
                        store[setting]["value"] = value
                        # store[setting]["type"] = show_type

            display, store = mk_settings(store)
            return store, display
        return no_update, no_update

    # Callback to update the page content
    @app.callback(
        Output("page-content", "children"),
        Input("url", "pathname"),
        State("settings-store", "data"),
        State("flux-table-col-store", "data"),
    )
    def display_page(pathname, settings_store, flux_col_store):
        logger.debug(settings_store)
        if pathname == f"{url}settings":
            page, _ = mk_settings(settings_store)
            page = mk_settings_page(page)
            return page
        if pathname == f"{url}db_view":
            logger.info("Making db view")
            logger.info(flux_col_store)
            page = mk_db_view_page(flux_col_store)
            return page
        else:
            page, _, _ = mk_main_page(
                settings_store["gas_graphs"], settings_store["attribute_graphs"]
            )
            return page

    # NOTE: add a file to record changes in the setup, so that chamber
    # selection buttons can be generated for currently available chambers
    # instead of always displaying all?
    @app.callback(
        Output("chamber-buttons", "children"),
        Input("output", "children"),
        # prevent_initial_call=True,
    )
    def generate_buttons(_):
        logger.info("Generating buttons")
        options = [{"label": chamber, "value": chamber} for chamber in chambers]
        # all = {"label": "All", "value": "all"}
        # options.insert(0, all)
        logger.debug(options)
        return dcc.Checklist(
            id="chamber-select",
            options=options,
            value=[],
            inline=True,
        )

    @app.callback(
        Output("flux-table-col-store", "data"),
        State("column-selector", "value"),
        Input("submit-table-cols", "n_clicks"),
    )
    def update_flux_col_store(selected_cols, submit_cols):
        if ctx.triggered_id == "submit-table-cols":
            return selected_cols
        return no_update

    @app.callback(
        Output("flux-db-tab", "children"),
        Output("cycle-db-tab", "children"),
        Output("gas-db-tab", "children"),
        Output("volume-db-tab", "children"),
        Output("meteo-db-tab", "children"),
        # Output("flux-table-col-store", "data", allow_duplicate=True),
        Input("db-view-tabs", "value"),
        State("flux-table-col-store", "data"),
        State("column-selector", "value"),
        prevent_initial_call=True,
    )
    def populate_db_views(tab, col_store, current_cols):
        table_cols = [{"label": col, "value": col} for col in col_store]
        logger.info(tab)
        if tab == "flux-db-tab":
            df = flux_table_to_df()
            col_select = dcc.Dropdown(
                id="column-selector",
                options=table_cols,
                value=list(col_store),  # Default: All columns selected
                multi=True,
            )
            datatable = dash_table.DataTable(
                id="flux-db-table",
                columns=[
                    {"name": col, "id": col, "deletable": True} for col in col_store
                ],
                data=df.to_dict("records"),
                sort_action="native",
                editable=True,
                page_size=50,
                style_data={"font-size": "15px"},
                style_header={"font-size": "15px"},
            )
            button = html.Button("Use cols", id="submit-table-cols")
            return (
                [button, col_select, datatable],
                no_update,
                no_update,
                no_update,
                no_update,
            )
        if tab == "cycle-db-tab":
            df = cycle_table_to_df("2021-01-01", "2025-01-01")
            datatable = dash_table.DataTable(
                id="cycle-db-table",
                columns=[{"name": col, "id": col} for col in df.columns],
                data=df.to_dict("records"),
                sort_action="native",
                page_size=50,
                style_table={"width": "80%", "margin": "auto"},
                style_data={"font-size": "18px"},
                style_header={"font-size": "18px"},
            )
            return no_update, [datatable], no_update, no_update, no_update
        if tab == "gas-db-tab":
            # df = gas_table_to_df()
            df = pd.DataFrame()
            datatable = dash_table.DataTable(
                id="gas-db-table",
                columns=[{"name": col, "id": col} for col in df.columns],
                data=df.to_dict("records"),
                sort_action="native",
                page_size=50,
                style_table={"width": "80%", "margin": "auto"},
                style_data={"font-size": "18px"},
                style_header={"font-size": "18px"},
            )
            placeholder = html.Div(
                "Need to add logic for querying limited range of data"
            )
            return no_update, no_update, [placeholder], no_update, no_update
        if tab == "volume-db-tab":
            df = volume_table_to_df()
            datatable = dash_table.DataTable(
                id="volume-db-table",
                columns=[{"name": col, "id": col} for col in df.columns],
                data=df.to_dict("records"),
                sort_action="native",
                page_size=50,
                style_table={"width": "80%", "margin": "auto"},
                style_data={"font-size": "18px"},
                style_header={"font-size": "18px"},
            )
            return no_update, no_update, no_update, [datatable], no_update
        if tab == "meteo-db-tab":
            df = meteo_table_to_df()
            datatable = dash_table.DataTable(
                id="meteo-db-table",
                columns=[{"name": col, "id": col} for col in df.columns],
                data=df.to_dict("records"),
                sort_action="native",
                page_size=50,
                style_table={"width": "80%", "margin": "auto"},
                style_data={"font-size": "18px"},
                style_header={"font-size": "18px"},
            )
            return no_update, no_update, no_update, no_update, [datatable]
        return no_update, no_update, no_update, no_update, no_update

    @app.callback(
        Output("date-store", "data"),
        Output("range-pick", "initial_visible_month"),
        Input("range-pick", "start_date"),
        Input("range-pick", "end_date"),
        State("date-store", "data"),
    )
    def store_range(range_start, range_end, stored_dates):
        # Parse current range
        date_range = parse_date_range(range_start, range_end)

        # Handle stored dates safely
        old_start = stored_dates.get("start_date") if stored_dates else None
        old_end = stored_dates.get("end_date") if stored_dates else None

        # Parse old date range only if needed
        if old_start and old_end:
            old_date_range = parse_date_range(old_start, old_end)
            old_start = old_date_range[0]
            old_end = old_date_range[1]

        # Return updated date store and initial visible month
        return (
            {
                "start_date": date_range[0],
                "end_date": date_range[1],
                "old_start": old_start,
                "old_end": old_end,
                "initial_visible_month": date_range[0],  # Use properly formatted month
            },
            date_range[0],
        )

    @app.callback(
        Output("range-pick", "start_date"),
        Output("range-pick", "end_date"),
        Input("date-store", "data"),
    )
    def update_range(date_range):
        if not date_range:
            return (None, None)
        return (date_range.get("start_date"), date_range.get("end_date"))

    @app.callback(
        Output({"type": "gas-graph", "index": ALL}, "figure"),
        Output({"type": "attrib-graph", "index": ALL}, "figure"),
        Output("measurement-info", "children"),
        Output("stored-index", "data", allow_duplicate=True),
        Output("stored-chamber", "data"),
        State({"type": "attrib-graph", "index": ALL}, "relayoutData"),
        Input({"type": "attrib-graph", "index": ALL}, "clickData"),
        Input({"type": "gas-graph", "index": ALL}, "relayoutData"),
        State("date-store", "data"),
        State("settings-store", "data"),
        Input("prev-button", "n_clicks"),
        Input("next-button", "n_clicks"),
        Input("skip-invalid", "value"),
        Input("skip-valid", "value"),
        Input("del-lagtime", "n_clicks"),
        Input("push-all", "n_clicks"),
        Input("push-single", "n_clicks"),
        Input("chamber-select", "value"),
        State("stored-index", "data"),
        State("stored-chamber", "data"),
        Input("mark-invalid", "n_clicks"),
        Input("mark-valid", "n_clicks"),
        Input("reset-cycle", "n_clicks"),
        Input("reset-index", "n_clicks"),
        Input("max-r", "n_clicks"),
        Input("run-init", "n_clicks"),
        Input("add-time", "n_clicks"),
        Input("substract-time", "n_clicks"),
        Input("parse-range", "n_clicks"),
        State("used-instrument-select", "value"),
        prevent_initial_call=True,
    )
    def update_graph(*args):
        stored_settings = args[4]
        logger.info(stored_settings)
        # points_store = args[-1]
        logger.debug(ctx.triggered_id)
        # print(args[1])

        graph_names = stored_settings["graph_names"]
        gas_relayouts = args[2]
        gas_graphs = stored_settings["graph_names"][0]
        attr_graphs = stored_settings["graph_names"][1]

        logger.debug("Running update")

        # the generate_buttons callbacks always triggers chamber-select, this
        # way we can ignore it and prevent the app from initiating twice
        # TODO: I guess this should be fixed by setting the chamber-select
        # checklist in the initial creation of the layout?
        # if None in args[0]:
        #     return no_data_response(chambers, [gas_graphs, attr_graphs], points_store)

        logger.info("Running")
        (
            triggered_elem,
            index,
            measurements,
            measurement,
            selected_chambers,
            date_range,
        ) = handle_triggers(args, ifdb_read_dict, chambers, graph_names)
        logger.debug("Handled triggers")
        if measurements is None or measurements.empty:
            return no_data_response(chambers, [gas_graphs, attr_graphs])

        execute_actions(
            triggered_elem,
            measurement,
            measurements,
            ifdb_read_dict,
            ifdb_push_dict,
            date_range,
        )

        figs = create_gas_plots(measurement, gas_graphs, stored_settings)

        # graph_names is a list of the ids of the graphs, first part of the id
        # is the attribute that is being plotted
        attrs = [name.split("-")[0] for name in attr_graphs]
        # create_attribute_graph takes in the attribute you want to plot and if
        # it is a gas specific attribute, also the name of the gas. If an attribute
        # is gas specific, its name can be generated by with <gas>_<attribute>
        # HACK: fix this properly by implementing instruments
        vars = [
            attr.split("_")[::-1]
            if attr.split("_")[0] in measurement.instrument.gases
            else [attr]
            for attr in attrs
        ]
        attr_plots = [
            create_attribute_graph(
                measurement,
                measurements,
                selected_chambers,
                index,
                triggered_elem,
                date_range,
                gas_graphs,
                *var,
            )
            for var in vars
        ]

        # TODO: add a toggle to zoom all rightside graph to the same width
        for i, graph in enumerate(attr_plots):
            apply_graph_zoom(graph, triggered_elem, args[0][i])

        # NOTE: when these elements are triggered we want to reset the zoom the main gas
        # graph, otherwise for some reason it can get reset from an old zoom
        # even after already going through multiple graphs
        # BUG: Zooming into gas graph, clicking mark invalid and then clicking
        # mark valid will mess up the zoom, double clicking the graph fixes it
        # though
        reset_gas = [
            "next-button",
            "prev-button",
            "skip-invalid",
            "skip-valid",
            "extend-time",
            "parse-range",
            "chamber-select",
            *attr_graphs,
        ]
        update = {"autosize": True}
        if triggered_elem in reset_gas:
            figs = [fig.update_layout(update) for fig in figs]
        else:
            for i, graph in enumerate(figs):
                apply_graph_zoom(graph, triggered_elem, gas_relayouts[i])

        all_meas = len(measurements)
        measurements.reset_index(inplace=True)
        current = measurements[measurements["start_time"] == index].index[0]
        formatted = f"{current+1:5}"
        html = formatted.replace(" ", "\u00a0")
        info_text = f"{html}/{all_meas} {str(measurement)}"
        return (
            figs,
            attr_plots,
            info_text,
            index,
            selected_chambers,
        )
