import logging
from flask import render_template, redirect, request, url_for, Response
from flask_login import login_required, current_user
from flask_restful import Resource

# from ac_depth import mk_ac_depth
from . import mk_ac_plot
from ac_dash.data_mgt import flux_table_to_df

from ac_dash.views.login import mk_login_page
from ac_dash.views.success import mk_success
from ac_dash.views.logout import mk_logout_page
from ac_dash.api.routes import register_api

from ac_dash.server import server, User, login_manager, api


logger = logging.getLogger("defaultLogger")


register_api(api)

ac_plot_route = "/dashing/"
# ac_depth = mk_ac_depth(server, "/ac_depth/")

mk_ac_plot(server, ac_plot_route)

login = mk_login_page(server, "/login/", User)
logout = mk_logout_page(server, "/logout/")
success = mk_success(server, "/success/")


@login_manager.user_loader
def user_loader(user_id):
    return User.query.get(user_id)


# @server.route("/home")
# @login_required
# def index():
#     return render_template("index.html", user=current_user)


@server.route("/")
def root():
    return redirect(ac_plot_route)


if __name__ == "__main__":
    server.run(host="0.0.0.0", debug=True)
