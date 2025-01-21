import os
from sqlalchemy import create_engine


# define project root
basedir = os.path.abspath(os.path.dirname(__file__))


# sqlalchemy config
class Config(object):
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite://")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


# initiate sqlalchemy engine
engine = create_engine(
    Config.SQLALCHEMY_DATABASE_URI,
    pool_size=10,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=1000,
)
