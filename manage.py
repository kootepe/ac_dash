from flask.cli import FlaskGroup, with_appcontext
import click

from ac_dash.server import server, db, User
from ac_dash.users_mgt.users_mgt import add_user as user_to_db
from ac_dash.data_mgt import delete_fluxes
from ac_dash import mk_ac_plot


cli = FlaskGroup(server)


@cli.command("create_db")
def create_db():
    db.drop_all()
    db.create_all()
    db.session.commit()


@cli.command("add_user")
@click.argument("username")
@click.argument("password")
@click.argument("email")
@click.argument("role")
def add_user(username, password, email, role):
    user_to_db(username, password, email, role)


@cli.command("seed_db")
def seed_db():
    db.session.add(User(email="eero.koskinen@oulu.fi"))
    db.session.commit()


@click.command("del_fluxes")
@click.argument("start")
@click.argument("end")
@with_appcontext
def del_fluxes(start, end):
    print(f"Deleting fluxes from {start} to {end}.")
    delete_fluxes(start, end)


cli.add_command(del_fluxes)

if __name__ == "__main__":
    cli()
