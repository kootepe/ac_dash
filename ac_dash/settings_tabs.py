import logging
import json
from dash import dcc, html
from .utils import load_config
from .data_mgt import get_distinct_meteo_source, get_instrument_rows_as_dicts

logger = logging.getLogger("defaultLogger")

upload_style = style = {
    "width": "20vw",
    "height": "3vw",
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
_, _, settings, _ = load_config()


def mk_init_tabs():
    distinct_source = get_distinct_meteo_source()
    logger.debug(distinct_source)

    distinct_source = [
        {
            "label": f"{item}",
            "value": json.dumps({"source": item}),
        }
        for item in distinct_source
    ]
    distinct_source_with_init = distinct_source + [
        {"label": "New source", "value": json.dumps({"source": "new"})}
    ]

    db_instruments = get_instrument_rows_as_dicts()

    for row in db_instruments:
        if row["serial"] == "":
            row["init"] = True
            row["label"] = f"New {row['python_class']}"
        else:
            row["label"] = f"{row['name']}"

    instruments = [
        {
            "label": f"{item['label']}",
            "value": json.dumps(item),
        }
        for item in db_instruments
    ]
    return [
        dcc.Tabs(
            children=[
                dcc.Tab(
                    label="Initiate from file.",
                    children=[
                        dcc.Tabs(
                            children=[
                                dcc.Tab(
                                    label="Upload gas measurements",
                                    children=[
                                        html.Div(
                                            [
                                                html.Label(
                                                    "Select which instrument you are uploading data from"
                                                ),
                                                dcc.Dropdown(
                                                    options=instruments,
                                                    multi=False,
                                                    id="instrument-select",
                                                    style={"width": "20vw"},
                                                ),
                                                html.Div(
                                                    id="model-input-warn",
                                                    style={
                                                        "background-color": "salmon",
                                                    },
                                                ),
                                                html.Div(
                                                    id="model-input-show",
                                                    style={
                                                        "background-color": "greenyellow",
                                                    },
                                                ),
                                                html.Div(
                                                    [
                                                        html.Label("Model"),
                                                        dcc.Input(
                                                            id="model-input",
                                                            style={
                                                                "width": "20vw",
                                                            },
                                                            disabled=True,
                                                        ),
                                                    ],
                                                    id="model-input-div",
                                                    style={
                                                        "display": "none",
                                                    },
                                                ),
                                                html.Div(
                                                    [
                                                        html.Label("Class"),
                                                        dcc.Input(
                                                            id="class-input",
                                                            style={
                                                                "width": "20vw",
                                                                "display": "none",
                                                            },
                                                        ),
                                                    ],
                                                    id="class-input-div",
                                                    style={
                                                        "display": "none",
                                                    },
                                                ),
                                                html.Div(
                                                    [
                                                        html.Label("Serial"),
                                                        dcc.Input(
                                                            id="serial-input",
                                                            style={
                                                                "width": "20vw",
                                                            },
                                                        ),
                                                    ],
                                                    id="serial-input-div",
                                                    style={
                                                        "display": "none",
                                                    },
                                                ),
                                                html.Div(
                                                    [
                                                        html.Label("Name"),
                                                        dcc.Input(
                                                            id="name-input",
                                                            style={
                                                                "width": "20vw",
                                                            },
                                                        ),
                                                    ],
                                                    id="name-input-div",
                                                    style={
                                                        "display": "none",
                                                    },
                                                ),
                                                dcc.Upload(
                                                    id="upload-data",
                                                    children=html.Div(
                                                        [
                                                            "Click to select files or drag and drop",
                                                            html.Br(),
                                                            html.Br(),
                                                            # html.A("Drag and drop or click to select files"),
                                                        ],
                                                        style=upload_style,
                                                    ),
                                                ),
                                            ],
                                            style={
                                                "display": "flex",
                                                "flex-direction": "column",
                                                "justify-content": "center",
                                                "align-items": "center",
                                                "padding": "15px",
                                                "background": "white",
                                                "border": "1px solid #ccc",
                                                "box-shadow": "0px 4px 6px rgba(0, 0, 0, 0.1)",
                                            },
                                        ),
                                    ],
                                    style={
                                        # "width": "45vw",
                                    },
                                ),
                                dcc.Tab(
                                    label="Upload chamber cycles",
                                    children=[
                                        html.Div(
                                            [
                                                html.Label(
                                                    "Select what type of cycles you are uploading"
                                                ),
                                                dcc.Dropdown(
                                                    options=[
                                                        {
                                                            "label": "Raw new protocol",
                                                            "value": "raw_new",
                                                        },
                                                        {
                                                            "label": "Raw old protocol",
                                                            "value": "raw_old",
                                                        },
                                                        {
                                                            "label": "Parsed protocol",
                                                            "value": "parsed",
                                                        },
                                                    ],
                                                    multi=False,
                                                    id="protocol-select",
                                                    style={"width": "20vw"},
                                                ),
                                                html.Div(
                                                    id="protocol-input-warn",
                                                    style={
                                                        "background-color": "salmon",
                                                    },
                                                ),
                                                html.Div(
                                                    id="protocol-input-show",
                                                    style={
                                                        "background-color": "greenyellow",
                                                    },
                                                ),
                                                dcc.Upload(
                                                    id="upload-protocol",
                                                    children=html.Div(
                                                        [
                                                            "Click to select log/csv/zip or drag and drop",
                                                        ],
                                                        style=upload_style,
                                                    ),
                                                ),
                                            ],
                                            style={
                                                "display": "flex",
                                                "flex-direction": "column",
                                                "justify-content": "center",
                                                "align-items": "center",
                                                "padding": "15px",
                                                "background": "white",
                                                "border": "1px solid #ccc",
                                                "box-shadow": "0px 4px 6px rgba(0, 0, 0, 0.1)",
                                            },
                                        ),
                                    ],
                                    style={
                                        # "width": "45vw",
                                    },
                                ),
                                dcc.Tab(
                                    label="Upload meteo data",
                                    children=[
                                        html.Div(
                                            [
                                                html.Label(
                                                    "Select what source of meteo data you are uploading"
                                                ),
                                                html.Div(
                                                    id="meteo-input-warn",
                                                    style={
                                                        "background-color": "salmon",
                                                    },
                                                ),
                                                html.Div(
                                                    id="meteo-input-show",
                                                    style={
                                                        "background-color": "greenyellow",
                                                    },
                                                ),
                                                dcc.Dropdown(
                                                    options=distinct_source_with_init,
                                                    # options=[
                                                    #     {
                                                    #         "label": "Oulanka fen",
                                                    #         "value": "oulanka_fen",
                                                    #     },
                                                    #     {
                                                    #         "label": "Oulanka pineforest",
                                                    #         "value": "oulanka_pineforest",
                                                    #     },
                                                    #     {
                                                    #         "label": "Oulanka genreal",
                                                    #         "value": "oulanka_general",
                                                    #     },
                                                    # ],
                                                    multi=False,
                                                    id="meteo-source-input",
                                                    style={
                                                        "width": "20vw",
                                                    },
                                                ),
                                                html.Div(
                                                    [
                                                        html.Label("Source name"),
                                                        dcc.Input(
                                                            # "New meteo source",
                                                            id="meteo-source-name",
                                                            disabled=True,
                                                            style={
                                                                "width": "20vw",
                                                            },
                                                        ),
                                                    ],
                                                    id="meteo-new-source-div",
                                                    style={
                                                        "display": "none",
                                                        "width": "20vw",
                                                    },
                                                ),
                                                dcc.Upload(
                                                    id="upload-meteo",
                                                    children=html.Div(
                                                        [
                                                            "Click to select csv/zip or drag and drop",
                                                        ],
                                                        style=upload_style,
                                                    ),
                                                ),
                                            ],
                                            style={
                                                "display": "flex",
                                                "flex-direction": "column",
                                                "justify-content": "center",
                                                "align-items": "center",
                                                "padding": "15px",
                                                "background": "white",
                                                "border": "1px solid #ccc",
                                                "box-shadow": "0px 4px 6px rgba(0, 0, 0, 0.1)",
                                            },
                                        ),
                                    ],
                                    style={
                                        # "width": "45vw",
                                    },
                                ),
                                dcc.Tab(
                                    label="Upload volume data",
                                    children=[
                                        html.Div(
                                            [
                                                html.Div(
                                                    id="volume-input-warn",
                                                    style={
                                                        "background-color": "salmon",
                                                    },
                                                ),
                                                html.Div(
                                                    id="volume-input-show",
                                                    style={
                                                        "background-color": "greenyellow",
                                                    },
                                                ),
                                                dcc.Upload(
                                                    id="upload-volume",
                                                    children=html.Div(
                                                        [
                                                            "Click to select csv/zip or drag and drop",
                                                        ],
                                                        style=upload_style,
                                                    ),
                                                ),
                                            ],
                                            style={
                                                "display": "flex",
                                                "flex-direction": "column",
                                                "justify-content": "center",
                                                "align-items": "center",
                                                "padding": "15px",
                                                "background": "white",
                                                "border": "1px solid #ccc",
                                                "box-shadow": "0px 4px 6px rgba(0, 0, 0, 0.1)",
                                            },
                                        ),
                                    ],
                                    style={
                                        # "width": "45vw",
                                    },
                                ),
                                dcc.Tab(
                                    label="Initiate fluxes",
                                    children=html.Div(
                                        [
                                            html.Div(
                                                id="init-flux-warn",
                                                style={"background-color": "salmon"},
                                            ),
                                            html.Label(
                                                "Select which instrument you are initiating calculations for"
                                            ),
                                            dcc.Dropdown(
                                                options=instruments,
                                                multi=False,
                                                id="flux-init-instrument-select",
                                                style={"width": "20vw"},
                                            ),
                                            html.Div(
                                                [
                                                    html.Label("Select meteo source"),
                                                    dcc.Dropdown(
                                                        id="flux-init-meteo-source",
                                                        options=distinct_source,
                                                        style={"width": "20vw"},
                                                    ),
                                                ]
                                            ),
                                            dcc.Input(
                                                placeholder="Start date",
                                                id="init-start",
                                                style={"width": "20vw"},
                                            ),
                                            dcc.Input(
                                                placeholder="End date",
                                                id="init-end",
                                                style={"width": "20vw"},
                                            ),
                                            html.Button("Calculate", id="init-flux"),
                                        ],
                                        style={
                                            "display": "flex",
                                            "flex-direction": "column",
                                            "justify-content": "center",
                                            "align-items": "center",
                                            "padding": "15px",
                                            "background": "white",
                                            "border": "1px solid #ccc",
                                            "box-shadow": "0px 4px 6px rgba(0, 0, 0, 0.1)",
                                        },
                                    ),
                                ),
                            ]
                        )
                    ],
                ),
                dcc.Tab(
                    label="Initiate from influxDB. -- not implemented yet",
                    children=[
                        html.Div(
                            [
                                html.Label("Soon tm"),
                            ],
                            style={
                                "display": "flex",
                                "flex-direction": "column",
                                "justify-content": "center",
                                "align-items": "center",
                                "padding": "15px",
                                "background": "white",
                                "border": "1px solid #ccc",
                                "box-shadow": "0px 4px 6px rgba(0, 0, 0, 0.1)",
                            },
                        )
                    ],
                ),
            ],
        )
    ]
