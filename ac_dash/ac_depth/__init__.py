from dash import Dash
from .layout import create_layout
from .callbacks import register_callbacks
import logging

from ..common_utils.utils import protect_dash_app
from ..utils import load_config

from flask import send_from_directory, session, abort
from pathlib import Path

logger = logging.getLogger("defaultLogger")


def mk_ac_depth(flask_app, url):
    app = Dash(__name__, server=flask_app, url_base_pathname=url)
    app.title = "Autochamber depth"

    HERE = Path(__file__).parent

    # HACK: These are a bit of a hack and should be implemented as somewhere
    # else at least... Maybe add the images in flask static folder? With an
    # implementation like this they are kept contained within ac_depth.. The
    # authentication implementation is also different compared to the dash apps

    # add path to ac depth instructions
    @app.server.route("/ac_depth/instructions")
    def get_instructions():
        if not is_authenticated_user():
            abort(403)
        return send_from_directory(HERE, "instructions.html")

    @app.server.route("/ac_depth/kammiot.png")
    def get_kammiot():
        if not is_authenticated_user():
            abort(403)
        return send_from_directory(HERE, "kammiot.png")

    chamber_blocks, _, _, measurement_spots = load_config()
    app.layout = create_layout(chamber_blocks, measurement_spots)
    register_callbacks(app, chamber_blocks, measurement_spots)

    protect_dash_app(flask_app, app)

    return app


def is_authenticated_user():
    return session.get("logged_in", False)
