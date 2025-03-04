from dash import html, dcc
from .data_mgt import Flux_tbl


def mk_db_view_page(columns=None):
    all_cols = [column.name for column in Flux_tbl.columns]
    if columns is None:
        columns = all_cols
    page = html.Div(
        [
            html.Div(
                [
                    html.H1("Db viewer"),
                    html.P(
                        "Work in progress, works for viewing but might have a bit bad of a user experience :-)"
                    ),
                    dcc.Tabs(
                        id="db-view-tabs",
                        value="flux-db-tab",
                        children=[
                            dcc.Tab(
                                label="View calculated flux data",
                                children=html.Div(
                                    id="tab-data",
                                    children=[
                                        html.Button("Use cols", id="submit-table-cols"),
                                        dcc.Dropdown(
                                            id="column-selector",
                                            options=[
                                                {"label": col, "value": col}
                                                for col in all_cols
                                            ],
                                            value=list(columns),
                                            multi=True,
                                        ),
                                    ],
                                    style={
                                        "display": "flexbox",
                                        "width": "80%",
                                    },
                                ),
                                value="flux-db-tab",
                                id="flux-db-tab",
                            ),
                            dcc.Tab(
                                label="View chamber cycle data",
                                # children=[dash_table.DataTable(id="cycle-db-table")],
                                value="cycle-db-tab",
                                id="cycle-db-tab",
                            ),
                            dcc.Tab(
                                label="View gas measurement data",
                                # children=[dash_table.DataTable(id="gas-db-table")],
                                value="gas-db-tab",
                                id="gas-db-tab",
                            ),
                            dcc.Tab(
                                label="View chamber volume measurement data",
                                # children=[dash_table.DataTable(id="volume-db-table")],
                                value="volume-db-tab",
                                id="volume-db-tab",
                            ),
                            dcc.Tab(
                                label="View meteo data",
                                # children=[dash_table.DataTable(id="meteo-db-table")],
                                value="meteo-db-tab",
                                id="meteo-db-tab",
                            ),
                        ],
                    ),
                ],
            )
        ],
    )
    return page
