from sqlalchemy import Table, text
from sqlalchemy.sql import select
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from ..db import engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

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


def check_existing_user(username):
    existing_user_query = f"""
        SELECT 1 FROM users WHERE username = '{username}';
        """
    with engine.connect() as conn:
        existing_user = conn.execute(text(existing_user_query)).fetchone()

    if existing_user:
        print(f"User {username} already exists.")
        return True


def add_user(username, password, email, role):
    if check_existing_user(username):
        return

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


def change_user_password(username, current_password, new_password):
    """
    Change the user's password in the database.

    Parameters
    ----------
    engine : sqlalchemy.Engine
        The SQLAlchemy database engine.

    username : str
        The username of the user.

    current_password : str
        The user's current password.

    new_password : str
        The new password to set.

    Returns
    -------
    dict
        A dictionary containing 'success' (bool) and 'message' (str).
    """

    # Create a session
    with Session(engine) as session:
        try:
            # Query the user from the database
            user = session.query(User).filter_by(username=username).first()

            if user is None:
                return {"success": False, "message": "User not found."}

            # Verify the current password
            if not check_password_hash(user.password, current_password):
                return {"success": False, "message": "Current password is incorrect."}

            # Hash the new password
            hashed_password = generate_password_hash(new_password)

            # Update the user's password in the database
            user.password = hashed_password
            session.commit()  # Commit the transaction

            return {"success": True, "message": "Password changed successfully."}
        except SQLAlchemyError as e:
            session.rollback()  # Roll back the transaction in case of an error
            return {"success": False, "message": f"An error occurred: {str(e)}"}
