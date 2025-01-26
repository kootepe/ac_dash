import logging
import json
import pandas as pd
from dash import (
    dcc,
    Output,
    dash_table,
    Input,
    State,
    ctx,
    no_update,
    html,
    ALL,
)


from .utils import (
    handle_triggers,
    no_data_response,
    execute_actions,
    create_gas_plots,
    create_attribute_graph,
    parse_date_range,
)
from .data_mgt import (
    cycle_table_to_df,
    flux_table_to_df,
    volume_table_to_df,
    meteo_table_to_df,
)
from .create_graph import apply_graph_zoom
from .layout import (
    mk_settings,
    mk_settings_page,
    mk_main_page,
    graph_style,
)
from .db_view_page import mk_db_view_page
from .data_mgt import Cycle_tbl, Flux_tbl
from .data_init import (
    read_gas_init_input,
    read_cycle_init_input,
    read_meteo_init_input,
    read_volume_init_input,
    init_flux,
)

cycle_tbl_cols = [col.name for col in Cycle_tbl.columns]

logger = logging.getLogger("defaultLogger")


def mk_binds(settings):
    binds = []
    for key, elem in settings.items():
        key_str = f"""{{"index":"{elem}","type":"logic-button"}}"""
        if elem == "next-button":
            key_str = elem
        if elem == "prev-button":
            key_str = elem

        # document.getElementById('{elem}').click()
        # document.getElementById('{key_str}').click()
        bind = f"""if (event.key == '{key}') {{
    document.getElementById('{key_str}').click()
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
    chambers,
    chamber_map,
    graph_names,
    settings,
):
    # bind h and l to previous and next buttons
    keybinds = settings["settings"]["keybinds"]
    app.clientside_callback(
        mk_binds(settings["settings"]["keybinds"]),
        *[
            Output(elem, "id")
            for key, elem in keybinds.items()
            if elem in ["prev-button", "next-button"]
        ],
        *[
            Output({"type": "logic-button", "index": elem}, "id")
            for key, elem in keybinds.items()
            if elem not in ["prev-button", "next-button"]
        ],
        *[
            Input(elem, "id")
            for key, elem in keybinds.items()
            if elem in ["prev-button", "next-button"]
        ],
        *[
            Input({"type": "logic-button", "index": elem}, "id")
            for key, elem in keybinds.items()
            if elem not in ["prev-button", "next-button"]
        ],
        # *[Input(elem, "id") for key, elem in settings["settings"]["keybinds"].items()],
    )

    @app.callback(
        Output("model-input-div", "style"),
        Output("model-input", "value"),
        Output("serial-input-div", "style"),
        Output("serial-input", "disabled"),
        Output("serial-input", "value"),
        Output("name-input-div", "style"),
        Output("name-input", "disabled"),
        Output("name-input", "value"),
        Output("class-input-div", "style"),
        Output("class-input", "disabled"),
        Output("class-input", "value"),
        Input("instrument-select", "value"),
        State("serial-input", "style"),
        State("settings-store", "data"),
        prevent_initial_call=True,
    )
    def show_instruments_for_gas_init(value, serial_style, stored):
        """
        Show instrument details based on the selected instrument.

        Parameters
        ----------
        value : str
            JSON string representing the selected instrument's details.

        serial_style : dict
            The current style of the serial input div.

        stored : dict
            Data stored in the settings-store.

        Returns
        -------
        tuple
            Updated styles, disabled states, and values for the model, serial,
            name, and class input components.
        """

        def create_return(
            serial_style,
            model_value,
            serial_disabled,
            serial_value,
            name_style,
            name_disabled,
            name_value,
            class_value,
        ):
            """Helper function to structure the return values."""
            return (
                serial_style,
                model_value,
                serial_style,
                serial_disabled,
                serial_value,
                name_style,
                name_disabled,
                name_value,
                no_update,
                False,
                class_value,
            )

        # Default behavior when no value is selected
        if value is None:
            return create_return(
                serial_style, "", False, "", serial_style, False, "", ""
            )

        value = json.loads(value)  # Parse the selected instrument details
        if value is None:  # Edge case if value is invalid JSON
            serial_style["display"] = "none"
            return create_return(
                serial_style, "", False, "", serial_style, False, "", ""
            )

        # Extract the instrument details
        needs_init = value.get("init", False)
        model = value.get("model", "")
        use_class = value.get("python_class", "")
        name = value.get("name", "")
        serial = value.get("serial", "")

        serial_style["display"] = "block"

        if needs_init:
            return create_return(
                serial_style, model, False, "", serial_style, False, "", use_class
            )
        else:
            return create_return(
                serial_style, model, True, serial, serial_style, True, name, use_class
            )

    @app.callback(
        Output("model-input-warn", "children"),
        Output("model-input-show", "children"),
        State("class-input", "value"),
        State("serial-input", "value"),
        State("model-input", "value"),
        State("name-input", "value"),
        Input("upload-data", "contents"),
        State("upload-data", "filename"),
        prevent_initial_call=True,
    )
    def gas_init_callback(use_class, serial, model, name, contents, filename):
        warn, show = read_gas_init_input(
            use_class, serial, model, name, contents, filename
        )
        return warn, show

    @app.callback(
        Output("protocol-input-warn", "children"),
        Output("protocol-input-show", "children"),
        Input("upload-protocol", "contents"),
        State("upload-protocol", "filename"),
        prevent_initial_call=True,
    )
    def cycle_init_callback(contents, filename):
        warn, show = read_cycle_init_input(contents, filename, chamber_map)
        return warn, show

    @app.callback(
        Output("meteo-input-warn", "children"),
        Output("meteo-input-show", "children"),
        State("meteo-source-input", "value"),
        Input("upload-meteo", "contents"),
        State("upload-meteo", "filename"),
        prevent_initial_call=True,
    )
    def meteo_init_callback(source, contents, filename):
        warn, show = read_meteo_init_input(source, contents, filename)
        return warn, show

    @app.callback(
        Output("volume-input-warn", "children"),
        Output("volume-input-show", "children"),
        State("volume-source-input", "value"),
        Input("upload-volume", "contents"),
        State("upload-volume", "filename"),
        prevent_initial_call=True,
    )
    def volume_init_callback(source, contents, filename):
        warn, show = read_volume_init_input(source, contents, filename)
        return warn, show

    @app.callback(
        Output("init-flux-warn", "children"),
        Input("init-flux", "n_clicks"),
        State("init-start", "value"),
        State("init-end", "value"),
        State("flux-init-instrument-select", "value"),
        State("flux-init-meteo-source", "value"),
        prevent_initial_call=True,
    )
    def init_flux_callback(init, start, end, instrument, meteo):
        if instrument is None:
            return "Select used instrument"
        if meteo is None:
            return "Select meteo source"
        if start is None:
            return "Give start date"
        if end is None:
            return "Give end date"
        try:
            pd.to_datetime(start, format="%Y-%m-%d")
            pd.to_datetime(end, format="%Y-%m-%d")
        except Exception:
            return "give YYYY-MM-DD date"
        warn = init_flux(init, start, end, instrument, meteo)
        return warn

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
        State("settings-store", "data"),
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

            logger.info(store)
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
            settings_dropdowns, _ = mk_settings(settings_store)
            page = mk_settings_page(settings_dropdowns, settings_store)
            return page
        if pathname == f"{url}db_view":
            logger.info("Making db view")
            logger.info(flux_col_store)
            page = mk_db_view_page(flux_col_store)
            return page
        else:
            page, _, _ = mk_main_page(
                settings_store["gas_graphs"],
                settings_store["attribute_graphs"],
                settings_store["instruments"],
                settings_store,
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
        Input("flux-table-col-store", "data"),
        State("column-selector", "value"),
        prevent_initial_call=True,
    )
    def populate_db_views(tab, col_store, current_cols):
        table_cols = [
            {"label": col.name, "value": col.name} for col in Flux_tbl.columns
        ]
        logger.info(tab)
        if tab == "flux-db-tab":
            df = flux_table_to_df(col_store)
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
        Input({"type": "logic-button", "index": ALL}, "n_clicks"),
        Input("prev-button", "n_clicks"),
        Input("next-button", "n_clicks"),
        Input("skip-invalid", "value"),
        Input("skip-valid", "value"),
        Input("chamber-select", "value"),
        State("stored-index", "data"),
        State("stored-chamber", "data"),
        Input("parse-range", "n_clicks"),
        Input("used-instrument-select", "value"),
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
        ) = handle_triggers(args, chambers, graph_names)
        logger.debug("Handled triggers")
        if measurements is None or measurements.empty:
            return no_data_response(chambers, [gas_graphs, attr_graphs])

        execute_actions(
            triggered_elem,
            measurement,
            measurements,
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
        all_meas = f"{all_meas+1:5}"
        all_meas = all_meas.replace(" ", "\u00a0")
        info_text = f"#{html}/{all_meas} {measurement.start_time}"
        info_text2 = mk_info_tbl(measurement)
        info_text = [info_text, info_text2]
        return (
            figs,
            attr_plots,
            info_text,
            index,
            selected_chambers,
        )


def mk_info_tbl(measurement):
    style = {
        "border": "1px solid #bbb",
        "font-size": "14px",
    }
    style_right = {
        "border": "1px solid #bbb",
        "font-size": "14px",
        "text-align": "right",
    }
    return html.Table(
        [
            # html.Tr(
            #     [
            #         html.Td("start_time", style=style),
            #         html.Td(measurement.start_time, style=style),
            #     ]
            # ),
            html.Tr(
                [
                    html.Td("Lag", style=style),
                    html.Td(f"{measurement.lagtime}s", style=style_right),
                ]
            ),
            html.Tr(
                [
                    html.Td("Chamber height", style=style),
                    html.Td(f"{measurement.chamber_height}m", style=style_right),
                ]
            ),
            html.Tr(
                [
                    html.Td("Temperature", style=style),
                    html.Td(
                        f"{round(measurement.air_temperature, 2)}c", style=style_right
                    ),
                ]
            ),
            html.Tr(
                [
                    html.Td("Errors", style=style),
                    html.Td(f"{measurement.error_string}", style=style_right),
                ]
            ),
        ],
        style={"border-collapse": "collapse"},
    )
