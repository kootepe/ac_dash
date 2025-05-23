from dash import dcc, html
from datetime import date
from datetime import timedelta
import logging
import json

from .settings_tabs import mk_init_tabs
from .data_mgt import Flux_tbl, get_instrument_rows_as_dicts

columns = [column.name for column in Flux_tbl.columns]

initial_date = date(2024, 11, 25)
start_date = initial_date - timedelta(days=7)
# start_date = date(2024, 11, 1)

logger = logging.getLogger("defaultLogger")


graph_style = {
    "height": "15vw",
    "width": "50vw",
    "max-height": "300px",
    "max-width": "900px",
}
upload_style = style = {
    "width": "30%",
    "height": "60px",
    "display": "flex",  # Flexbox for alignment
    "flexDirection": "column",  # Stack items vertically
    "justifyContent": "center",  # Center items vertically
    "alignItems": "center",  # Center items horizontally
    "borderWidth": "1px",
    "borderStyle": "dashed",
    "borderRadius": "5px",
    "textAlign": "center",
    "margin": "10px",
    "padding": "10px",  # Add space inside the box
}


def mk_main_page(left_settings, right_settings, settings):
    db_instruments = get_instrument_rows_as_dicts()

    # if serial is empty, there's no data for it
    db_instruments = [row for row in db_instruments if row["serial"] != ""]

    dd_items = [
        {"label": item["name"], "value": json.dumps(item)} for item in db_instruments
    ]

    left_graphs = [
        dcc.Graph(
            id={"type": "gas-graph", "index": f"{graph_name}-plot"},
            style=graph_style,
            config={"edits": {"shapePosition": True}},
        )
        for graph_name in left_settings["value"]
    ]

    right_graphs = [
        dcc.Graph(
            id={"type": "attrib-graph", "index": f"{graph_name}-graph"},
            style=graph_style,
        )
        for graph_name in right_settings["value"]
    ]
    graph_names = [
        [f"{gas}-plot" for gas in left_settings["value"]],
        [f"{attribute}-graph" for attribute in right_settings["value"]],
    ]

    main_page = html.Div(
        [
            html.Div(
                [
                    dcc.Store(
                        id="date-store",
                        data={"start_date": start_date, "end_date": initial_date},
                        storage_type="local",
                    ),
                    dcc.Store(
                        id="measurement-store",
                        data={},
                        storage_type="local",
                    ),
                ]
            ),
            html.Div(
                [
                    dcc.DatePickerRange(
                        id="range-pick",
                        display_format="YYYY-MM-DD",
                        min_date_allowed=date(2018, 1, 1),
                        max_date_allowed=date(2030, 1, 1),
                        initial_visible_month=initial_date,
                        start_date=start_date,
                        end_date=initial_date,
                    ),
                    html.Button("Use range", id="parse-range"),
                ]
            ),
            html.Div([dcc.Checklist(id="chamber-select")], id="chamber-buttons"),
            html.Div(
                [
                    dcc.Checklist(
                        options=[{"label": "Hide invalids", "value": 1}],
                        id="skip-invalid",
                        style={"width": "150px"},
                    ),
                    dcc.Checklist(
                        options=[{"label": "Hide Valids", "value": 1}],
                        id="skip-valid",
                        style={"width": "150px"},
                    ),
                ],
            ),
            html.Div(
                [
                    dcc.Dropdown(
                        options=dd_items,
                        persistence=True,
                        persisted_props=["value"],
                        persistence_type="local",
                        multi=False,
                        id="used-instrument-select",
                        style={"width": "20vw"},
                    ),
                ]
            ),
            html.Div(
                children=[
                    html.Div(
                        [
                            html.Button("Previous", id="prev-button", n_clicks=0),
                            html.Button("Next", id="next-button", n_clicks=0),
                        ],
                        id="nav-buttons",
                    ),
                    html.Div(
                        [
                            html.Button(
                                item["text"],
                                id={"type": "logic-button", "index": key},
                                n_clicks=0,
                                style={"padding": "1px"},
                            )
                            for key, item in settings["layout_buttons"].items()
                        ],
                        id="logic-buttons",
                    ),
                ],
                style={"margin": "5px"},
            ),
            html.Div(
                id="measurement-info",
                style={
                    "font-family": "monospace",
                    "font-size": "14px",
                    "padding": "5px 0",
                },
            ),
            html.Div(
                [
                    html.Div(left_graphs),
                    html.Div(right_graphs),
                ],
                id="graph-div",
                style={"display": "flex"},
            ),
            html.Div(id="output"),
            dcc.Store(id="stored-index", data=0, storage_type="local"),
            dcc.Store(id="stored-chamber", data="All"),
            dcc.Store(id="stored-measurement-date"),
            dcc.Store(id="point-store", data="init", storage_type="local"),
            dcc.Store(id="relayout-data", data=None),
        ]
    )
    return main_page, graph_names, (left_graphs, right_graphs)


def mk_settings(settings_json):
    settings_dropdowns = []
    stored_settings = {}
    for key, setting in settings_json.items():
        # if setting.get("type", None) is None and isinstance(setting, dict):
        logger.debug(setting)
        if not isinstance(setting, dict) or setting.get("type", None) is None:
            stored_settings[key] = setting
            continue
        logger.debug(setting)

        if setting.get("type", None) is not None:
            if setting["type"] == "dropdown":
                dd = create_dropdown(setting, key)
                settings_dropdowns.append(dd)
                stored_settings[key] = setting
    return settings_dropdowns, stored_settings


def create_dropdown(settings, name):
    """Create a dropdown component."""
    options = settings["opts"]
    value = settings["value"]
    multi = settings["multi"]
    text = settings["text"]
    id = name

    dropdown = dcc.Dropdown(
        options=[{"label": key, "value": key} for key in options],
        value=value,
        id=id,
        multi=True if multi == 1 else False,
    )
    return html.Div(
        [html.Label(f"{text}", htmlFor=id), dropdown], style={"width": "500px"}
    )


cell_border = {
    "border": "1px solid #bbb",
    "font-size": "14px",
}

input_style = {"outline": "none", "border": "none"}


def mk_settings_page(settings_elems, settings_json):
    def mk_row(button):
        return html.Tr(
            [
                html.Td(
                    button,
                    style=cell_border,
                ),
                html.Td(
                    dcc.Input(
                        placeholder="Give keycode",
                        style=input_style,
                    ),
                    style=cell_border,
                ),
            ],
            style={"line-height": "15px"},
        )

    logger.debug(settings_json)
    logger.debug(settings_json["layout_buttons"])
    children = [
        html.Div(children=[html.Label(item["text"]), dcc.Input()])
        for key, item in settings_json["layout_buttons"].items()
    ]
    children = [
        mk_row(item["text"]) for key, item in settings_json["layout_buttons"].items()
    ]
    header = [
        html.Tr(
            children=[
                html.Td(
                    "Element",
                    style={
                        "border": "none",
                        "font-size": "14px",
                        "font-weight": "bold",
                    },
                ),
                html.Td(
                    "Key to bind",
                    style={
                        "border": "none",
                        "font-size": "14px",
                        "font-weight": "bold",
                    },
                ),
            ]
        ),
    ]
    children = header + children

    keybinds = html.Div(
        [
            # Input to bind keys to specific actions
            html.Div(
                [
                    html.Div(
                        children=html.Table(
                            *[children],
                            style={"border-collapse": "collapse"},
                        )
                    ),
                ]
            ),
            # Storage for bindings
            dcc.Store(id="key-bindings", data={}),
            # Actions
        ]
    )
    page = html.Div(
        [
            html.H1("Settings Page"),
            html.Div(
                [
                    html.Button("Download calculated fluxes", id="dl-all-button"),
                    dcc.Download(id="dl-all"),
                ],
                style={"padding-bottom": "20px"},
            ),
            dcc.Tabs(
                [
                    dcc.Tab(
                        label="General Settings",
                        children=[
                            html.Div(settings_elems, id="show-settings"),
                            html.Div(
                                [html.Button("Save settings", id="save-settings")]
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="Keybinds -- not implemented yet",
                        children=[keybinds],
                    ),
                    dcc.Tab(label="Data initiation", children=mk_init_tabs()),
                ],
            ),
        ]
    )
    return page


def create_layout(layout_json, url):
    settings_json = layout_json["settings"]
    logger.debug("Creating layout.")
    left_graphs = settings_json["gas_graphs"]
    right_graphs = settings_json["attribute_graphs"]

    settings_dropdowns, stored_settings = mk_settings(settings_json)
    settings_page = mk_settings_page(settings_dropdowns, settings_json)

    main_page, graph_names, graphs = mk_main_page(
        left_graphs,
        right_graphs,
        settings_json,
    )
    stored_settings["graph_names"] = graph_names
    logout = html.A("Log out", href="/logout")

    layout = html.Div(
        [
            dcc.Location(id="url", refresh=False),
            dcc.Store(id="settings-store", data=stored_settings, storage_type="local"),
            dcc.Store("flux-table-col-store", data=columns, storage_type="local"),
            html.Div(id="instrument-init", style={"display": "none"}),
            html.A("Go to Home Page", href="/", style={"padding-right": "15px"}),
            dcc.Download(id="dl-template"),
            dcc.Link("Go to app", href=url, style={"padding-right": "15px"}),
            dcc.Link(
                "Go to Settings",
                href=f"{url}settings",
                style={"padding-right": "15px"},
            ),
            dcc.Link(
                "Go to db view",
                href=f"{url}db_view",
                style={"padding-right": "15px"},
            ),
            dcc.Link(
                "Change password",
                href=f"{url}changepw",
                style={"padding-right": "15px"},
            ),
            html.Div(logout),
            html.Div(id="page-content"),
        ]
    )

    return layout, main_page, settings_page, graph_names
