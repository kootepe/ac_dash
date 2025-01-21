import requests
import flask
from dash import Dash, no_update
from dash import dcc, html
from dash.dependencies import Input, Output, State

from flask_login import login_user
from ..server import User
from flask import session
from werkzeug.security import check_password_hash


def mk_login_page(flask_app, url):
    app = Dash(__name__, server=flask_app, url_base_pathname=url)
    app.title = "Login"
    app.layout = html.Div(
        children=[
            html.Div(
                className="container",
                children=[
                    dcc.Location(id="url_login", refresh=True),
                    html.Div("""Please log in to continue:""", id="h1"),
                    html.Div(
                        # method='Post',
                        children=[
                            dcc.Input(
                                placeholder="Enter your username",
                                n_submit=0,
                                type="text",
                                id="uname-box",
                            ),
                            dcc.Input(
                                placeholder="Enter your password",
                                n_submit=0,
                                type="password",
                                id="pwd-box",
                            ),
                            html.Button(
                                children="Login",
                                n_clicks=0,
                                type="submit",
                                id="login-button",
                            ),
                            html.Div(children="", id="output-state"),
                        ]
                    ),
                ],
            )
        ]
    )

    @app.callback(
        Output("url_login", "pathname"),
        Output("output-state", "children"),
        [
            Input("login-button", "n_clicks"),
            Input("uname-box", "n_submit"),
            Input("pwd-box", "n_submit"),
        ],
        [State("uname-box", "value"), State("pwd-box", "value")],
        prevent_initial_call=True,
    )
    def handle_login(n_clicks, n_submit_uname, n_submit_pwd, username, password):
        if n_clicks > 0 or n_submit_uname > 0 or n_submit_pwd > 0:
            try:
                # Make a POST request to the Flask login route
                user = User.query.filter_by(username=username).first()
                if check_password_hash(user.password, password):
                    login_user(user)
                    session["logged_in"] = True
                    return "/", ""
                else:
                    return no_update, "Wrong username or password"
            except Exception:
                return "", "Error occurred"

    # @app.callback(
    #     Output("url_login", "pathname"),
    #     [
    #         Input("login-button", "n_clicks"),
    #         Input("uname-box", "n_submit"),
    #         Input("pwd-box", "n_submit"),
    #     ],
    #     [State("uname-box", "value"), State("pwd-box", "value")],
    # )
    # def success(n_clicks, n_submit_uname, n_submit_pwd, input1, input2):
    #     user = uname.query.filter_by(username=input1).first()
    #     if user:
    #         if check_password_hash(user.password, input2):
    #             login_user(user)
    #             session["logged_in"] = True
    #             return "/"
    #         else:
    #             pass
    #     else:
    #         pass

    # @app.callback(
    #     Output("output-state", "children"),
    #     [
    #         Input("login-button", "n_clicks"),
    #         Input("uname-box", "n_submit"),
    #         Input("pwd-box", "n_submit"),
    #     ],
    #     [State("uname-box", "value"), State("pwd-box", "value")],
    # )
    # def update_output(n_clicks, n_submit_uname, n_submit_pwd, input1, input2):
    #     if n_clicks > 0 or n_submit_uname > 0 or n_submit_pwd > 0:
    #         user = uname.query.filter_by(username=input1).first()
    #         if user:
    #             if check_password_hash(user.password, input2):
    #                 return ""
    #             else:
    #                 return "Incorrect username or password"
    #         else:
    #             return "Incorrect username or password"
    #     else:
    #         return ""
    #
    # return app
