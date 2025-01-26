from flask import Flask
from flask_login import current_user
from dash import Dash
from dash import dcc, html
from dash.dependencies import Input, Output, State


from ..users_mgt.users_mgt import change_user_password


def mk_change_pw(flask_app=None, url="/changepw/", username=None):
    if flask_app is None:
        flask_app = Flask(__name__)

    app = Dash(__name__, server=flask_app, url_base_pathname=url)
    app.title = "Change password"
    app.layout = html.Div(
        [
            dcc.Location(id="change-pw", refresh=True),
            html.H1(
                f"Change password for user {username}",
                id="header",
                style={"textAlign": "center"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("Current password"),
                                    dcc.Input(
                                        id="current-password",
                                        type="password",
                                        placeholder="Current Password",
                                        style={"margin": "10px"},
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.Label("New password"),
                                    dcc.Input(
                                        id="new-password",
                                        type="password",
                                        placeholder="New Password",
                                        style={"margin": "10px"},
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.Label("Confirm password"),
                                    dcc.Input(
                                        id="confirm-password",
                                        type="password",
                                        placeholder="Confirm New Password",
                                        style={"margin": "10px"},
                                    ),
                                ]
                            ),
                        ]
                    ),
                    html.Button(
                        "Change Password",
                        id="change-password-btn",
                        n_clicks=0,
                        style={"margin": "10px"},
                    ),
                    html.Div(
                        id="change-password-output",
                        style={"marginTop": "20px", "color": "red"},
                    ),
                ],
                style={
                    "maxWidth": "400px",
                    "margin": "auto",
                    "padding": "20px",
                    "border": "1px solid #ccc",
                    "borderRadius": "5px",
                },
            ),
        ]
    )

    @app.callback(
        Output("change-password-output", "children"),
        Input("change-password-btn", "n_clicks"),
        State("current-password", "value"),
        State("new-password", "value"),
        State("confirm-password", "value"),
    )
    def change_password(n_clicks, current_password, new_password, confirm_password):
        if n_clicks > 0:
            # Validate input
            if not current_password or not new_password or not confirm_password:
                return "All fields are required."

            if new_password != confirm_password:
                return "New password and confirmation do not match."

            # Password strength validation (optional)
            if (
                len(new_password) < 8
                # or not re.search(r"\d", new_password)
                # or not re.search(r"[A-Z]", new_password)
            ):
                return "Password must be at least 8 characters long, include a number and an uppercase letter."

            # Simulate username from the current user session (replace this with actual session handling)
            username = current_user.username

            # Call the mock function to update the password in the database
            response = change_user_password(username, current_password, new_password)

            if response["success"]:
                return html.Div(
                    "Password changed successfully!", style={"color": "green"}
                )
            else:
                return f"Failed to change password. {response['message']}"

        return ""

    return app
