import uuid
import pytz
import json

from flask_login import login_required

LOCAL_TZ = pytz.timezone("Europe/Helsinki")
CONTAINER_TZ = pytz.timezone("UTC")


def mk_uuid():
    """Generate a short UUID."""
    return str(uuid.uuid4())[:10]


def protect_dash_app(flask_app, dash_app):
    """
    Add log in requirement to dash application.
    """
    for view_func in flask_app.view_functions:
        if view_func.startswith(dash_app.config["url_base_pathname"]):
            flask_app.view_functions[view_func] = login_required(
                flask_app.view_functions[view_func]
            )
    return flask_app


def load_project_and_config():
    """Load project data and config from JSON files."""
    with open("project/config/projects.json", "r") as f:
        projects_json = json.load(f)
        projects = projects_json["PROJECTS"]

    with open("project/config/maintenance_log_config.json", "r") as f:
        config = json.load(f)
        ifdb_dict = config["CONFIG"]["INFLUXDB"]

    return projects, ifdb_dict
