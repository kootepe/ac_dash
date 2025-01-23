import logging
import json
from dash import dcc, html
from .measuring import class_model_key
from .utils import load_config

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
_, _, _, _, settings = load_config()

generic_instruments = [
    {
        "label": f"Generic {key}",
        "value": json.dumps({f"Generic {key}": {"class": key}}),
    }
    for key, item in class_model_key.items()
]


upload_instruments = [
    {"label": f"{key}", "value": json.dumps({key: item})}
    for key, item in settings["upload_instruments"].items()
]
custom_instruments = [
    {"label": f"{key}", "value": json.dumps(item)}
    for key, item in settings["instruments"].items()
]

instruments = upload_instruments + generic_instruments

data_init_tabs = [
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
                                                "Select what type of cycles you are uploading"
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
                                                options=[
                                                    {
                                                        "label": "Oulanka fen",
                                                        "value": "oulanka_fen",
                                                    },
                                                    {
                                                        "label": "Oulanka pineforest",
                                                        "value": "oulanka_pineforest",
                                                    },
                                                    {
                                                        "label": "Oulanka genreal",
                                                        "value": "oulanka_general",
                                                    },
                                                ],
                                                multi=False,
                                                id="meteo-source-input",
                                                style={"width": "20vw"},
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
                                label="Upload volume data -- not implemented yet",
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
                                        dcc.Input(
                                            placeholder="Start date", id="init-start"
                                        ),
                                        dcc.Input(
                                            placeholder="End date", id="init-end"
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
