import os
from sqlalchemy import create_engine


class Config(object):
    # SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite://")
    SQLALCHEMY_DATABASE_URI = (
        "postgresql://hello_flask:hello_flask@db:5432/hello_flask_dev"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False


# initiate sqlalchemy engine
engine = create_engine(
    Config.SQLALCHEMY_DATABASE_URI,
    pool_size=10,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=1000,
)
