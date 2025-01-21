import warnings
from dash import Dash

# Dash configuration
from dash import dcc, html
from dash.dependencies import Input, Output


warnings.filterwarnings("ignore")


def mk_success(flask_app, url):
    app = Dash(__name__, server=flask_app, url_base_pathname=url)
    # Create success layout
    app.layout = html.Div(
        children=[
            dcc.Location(id="url_login_success", refresh=True),
            html.Div(
                className="container",
                children=[
                    html.Div(
                        html.Div(
                            className="row",
                            children=[
                                html.Div(
                                    className="ten columns",
                                    children=[
                                        html.Br(),
                                        html.Div("Login successfull"),
                                    ],
                                ),
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
    @app.callback(
        Output("url_login_success", "pathname"), [Input("back-button", "n_clicks")]
    )
    def logout_dashboard(n_clicks):
        if n_clicks > 0:
            return "/"

    return app
