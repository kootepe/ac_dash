from flask import Flask
from dash import Dash


from .tools.logger import init_logger
from .layout import create_layout
from .utils import (
    load_config,
)
from .callbacks import register_callbacks
import logging
from .common_utils.utils import protect_dash_app


logger = logging.getLogger("defaultLogger")


def mk_ac_plot(flask_app=None, url="/ac_dash/"):
    if flask_app is None:
        flask_app = Flask(__name__)

    app = Dash(
        __name__,
        server=flask_app,
        url_base_pathname=url,
    )
    app.title = "Chamber validator"
    # auth = BasicAuth(app, users)
    protect_dash_app(flask_app, app)

    init_logger()
    # Load configuration and cycles
    (chambers, chamber_map, layout_json, _) = load_config()
    # Set up layout
    app.layout, main_page, settings_page, graph_names = create_layout(layout_json, url)

    # flatten chamber list
    chambers = [item for row in chambers for item in row]
    # Register callbacks
    register_callbacks(
        app,
        url,
        main_page,
        settings_page,
        chambers,
        chamber_map,
        graph_names,
        layout_json,
    )

    return app
