from dash import dcc, html, dash_table
from datetime import datetime as dt

from .utils import LOCAL_TZ, CONTAINER_TZ


chamber_style = {
    # "outline": "2px solid black",
    # "margin": "15px",
    "padding": "2px",
}
block_style = {
    # "outline": "2px solid black",
    "padding": "5px",
    "margin": "5px",
    "display": "flex",
    "flexDirection": "column",
}


def create_chamber_components(chamber_blocks, measurement_spots):
    chamber_style = {
        "display": "block",
        # "margin": "15px",
        "padding": "2px",
    }
    components = []
    for chamber_list in chamber_blocks:
        # block of chambers
        blocks = []

        for placeholder in chamber_list:
            blocks.append(
                html.Div(
                    html.Div(
                        children=[
                            html.Div(f"Chamber {placeholder}"),
                            *[
                                html.Div(
                                    dcc.Input(
                                        placeholder=spot,
                                        type="number",
                                        id={
                                            "type": f"{spot.lower()}-submit",
                                            "index": placeholder,
                                        },
                                        style={
                                            "display": "flex",
                                            # "width": "15vw",
                                        },
                                    )
                                )
                                for spot in measurement_spots
                            ],
                            html.Button(
                                "Submit",
                                id={"type": "submit", "index": placeholder},
                            ),
                        ],
                        style=chamber_style,
                    ),
                ),
            )
        components.append(html.Div(children=blocks, style=block_style))

    return components


upload_style = style = {
    # "width": "20vw",
    "height": "fit-content",
    "display": "flex",  # Flexbox for alignment
    # "white-space": "nowrap",
    "text-align": "center",
    "flexDirection": "column",  # Stack items vertically
    "borderWidth": "1px",
    "borderStyle": "dashed",
    "borderRadius": "5px",
    "margin-left": "15px",
    "margin": "6px",
    "margin-top": "1px",
    "padding": "12px",  # Add space inside the box
}


def create_layout(chamber_blocks, measurement_spots):
    timeinput = {"display": "inline-block", "margin": "0px"}
    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        html.A(
                            "View instructions for measuring.",
                            href="/ac_depth/instructions",
                            target="_blank",
                        ),
                        style={"margin-bottom": "10px"},
                    ),
                    html.Div(
                        [
                            html.Button(
                                "Download template file",
                                id="btn-template",
                                className="template-but",
                                style={
                                    "height": "fit-content",
                                    "padding": "10px",
                                },
                            ),
                            dcc.Download(id="dl-template"),
                            dcc.Upload(
                                id="up-template",
                                className="template-but",
                                children=html.Div(
                                    [
                                        "Upload template file",
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
                            "width": "100vw",
                        },
                    ),
                ],
                style={"width": "50vw"},
            ),
            html.P("Input Oulanka autochamber heights here."),
            html.Div(
                children=[
                    html.Div(id="warning-div"),
                    html.Div(id="csv-warning-div"),
                    html.Div(id="all-upload-warning-div"),
                ],
                style={"backgroundColor": "salmon", "width": "fit-content"},
            ),
            html.Div(
                children=[
                    html.Div(id="all-upload-text-div"),
                    html.Div(id="dummy-div"),
                ],
                style={"backgroundColor": "greenyellow", "width": "fit-content"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            dash_table.DataTable(
                                id="data-table",
                                data=[],
                                columns=[],
                                style_cell={
                                    "padding": "10px",
                                    "textAlign": "left",
                                    "border": "1px solid black",
                                    "width": "100%",
                                },
                                page_size=15,
                                editable=True,
                            ),
                            html.Div(id="upload-table-div"),
                            html.Button("Upload data", id="upload-all"),
                        ],
                        id="upload-display",
                        style={"display": "none"},
                    ),
                ],
            ),
            html.Div(
                [
                    dcc.Checklist(
                        id="has-snow",
                        options=[
                            {
                                "label": "Snow in chamber?",
                                "value": 1,
                            }
                        ],
                        value=[1],
                    ),
                    html.Div(
                        [
                            html.Div(
                                html.Table(
                                    [
                                        html.Tr(
                                            [
                                                html.Td("Date YY-MM-DD HH:MM"),
                                                html.Td(
                                                    dcc.Input(
                                                        dt.strftime(
                                                            CONTAINER_TZ.localize(
                                                                dt.now()
                                                            ).astimezone(LOCAL_TZ),
                                                            format="%Y-%m-%d %H:%M",
                                                        ),
                                                        id="date-input",
                                                        className="metadata-input",
                                                        # style={"width": "20vw"},
                                                    ),
                                                ),
                                            ]
                                        ),
                                        html.Tr(
                                            [
                                                html.Td("Measurement unit"),
                                                html.Td(
                                                    dcc.Input(
                                                        value="mm",
                                                        id="measurement-unit",
                                                        className="metadata-input",
                                                        # style={"width": "20vw"},
                                                    )
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ),
                        ],
                        # style={"width": "100vw"},
                    ),
                ]
            ),
            html.Div(
                [
                    *create_chamber_components(chamber_blocks, measurement_spots),
                    html.Div(
                        [
                            html.Button(
                                id={"type": "del-button", "id": "init", "index": 0},
                                style={"display": "none"},
                            )
                        ],
                        id="text-div",
                        style={"margin": "5px"},
                    ),
                ],
                style={
                    "display": "flex",
                },
            ),
        ],
    )
