from sqlalchemy import Table
from sqlalchemy.sql import select
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash
from ..db import engine

db = SQLAlchemy()


class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(15), unique=True)
    email = db.Column(db.String(50), unique=True)
    password = db.Column(db.String(200))
    role = db.Column(db.String(10))


User_tbl = Table("users", User.metadata)


def mk_user_table():
    User.metadata.create_all(engine)


def add_user(username, password, email, role):
    hashed_password = generate_password_hash(password)

    ins = User_tbl.insert().values(
        username=username, email=email, password=hashed_password, role=role
    )
    print(f'Adding user "{username}".')

    with engine.begin() as conn:
        conn.execute(ins)


def del_user(username):
    delete = User_tbl.delete().where(User_tbl.c.username == username)

    conn = engine.connect()
    conn.execute(delete)
    conn.close()


def show_users():
    select_st = select(User_tbl.c.username, User_tbl.c.email)

    conn = engine.connect()
    rs = conn.execute(select_st)

    for row in rs:
        print(row)

    conn.close()
