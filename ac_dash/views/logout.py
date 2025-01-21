# Dash configuration
from dash import Dash, dcc, html, Input, Output
from flask_login import logout_user


def mk_logout_page(flask_app, url):
    app = Dash(__name__, server=flask_app, url_base_pathname=url)
    app.title = "Logout"
    # Create app layout
    app.layout = html.Div(
        children=[
            dcc.Location(id="url_logout", refresh=True),
            dcc.Location(id="url_back", refresh=True),
            html.Div(
                className="container",
                children=[
                    html.Div(
                        html.Div(
                            className="row",
                            children=[
                                # html.Div(
                                #     className="ten columns",
                                #     children=[
                                #         html.Br(),
                                #         html.Div("User logged out."),
                                #     ],
                                # ),
                                html.Div(
                                    className="two columns",
                                    # children=html.A(html.Button('LogOut'), href='/')
                                    children=[
                                        html.Br(),
                                        html.Button(
                                            id="back-button",
                                            children="Go back",
                                            n_clicks=0,
                                        ),
                                        html.Button(
                                            id="log-out",
                                            children="Click to log out.",
                                            n_clicks=0,
                                        ),
                                    ],
                                ),
                            ],
                        )
                    )
                ],
            ),
        ]
    )

    # Create callbacks
    @app.callback(Output("url_back", "pathname"), [Input("back-button", "n_clicks")])
    def logout_dashboard(n_clicks):
        if n_clicks > 0:
            return "/"

    @app.callback(Output("url_logout", "pathname"), [Input("log-out", "n_clicks")])
    def logout(n_clicks):
        if n_clicks > 0:
            logout_user()
            return "/"
