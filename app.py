import os
import secrets
from flask import redirect, Blueprint
from flask_login import LoginManager
from ac_dash import mk_ac_plot
from ac_dash.api.routes import register_api, auth_bp
from ac_dash.server import server, User, login_manager, api
from ac_dash.views.login import mk_login_page
from ac_dash.views.success import mk_success
from ac_dash.views.logout import mk_logout_page


ac_plot_route = "/ac_dash/"
mk_ac_plot(server, ac_plot_route)

register_api(api)


mk_login_page(server, "/login/")
mk_logout_page(server, "/logout/")
mk_success(server, "/success/")

server.register_blueprint(auth_bp)


@server.route("/")
def root():
    return redirect(ac_plot_route)


@login_manager.user_loader
def user_loader(user_id):
    return User.query.get(user_id)


if __name__ == "__main__":
    server.run(debug=True)
