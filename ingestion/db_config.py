import os

import psycopg2
from sqlalchemy import create_engine


def _get_database_url():
    database_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")

    if database_url and database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    return database_url


def _get_sslmode(database_url):
    db_sslmode = os.getenv("DB_SSLMODE")

    if db_sslmode:
        return db_sslmode

    if database_url and "proxy.rlwy.net" in database_url:
        return "require"

    return None


def get_psycopg2_connection():
    database_url = _get_database_url()
    sslmode = _get_sslmode(database_url)

    if database_url:
        connect_kwargs = {"dsn": database_url}

        if sslmode:
            connect_kwargs["sslmode"] = sslmode

        return psycopg2.connect(**connect_kwargs)

    connect_kwargs = {
        "dbname": os.getenv("DB_NAME", "educacao"),
        "user": os.getenv("DB_USER", "mairasoaressales"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    db_password = os.getenv("DB_PASSWORD")
    if db_password:
        connect_kwargs["password"] = db_password

    if sslmode:
        connect_kwargs["sslmode"] = sslmode

    return psycopg2.connect(**connect_kwargs)


def get_sqlalchemy_engine():
    database_url = _get_database_url()

    if not database_url:
        db_user = os.getenv("DB_USER", "mairasoaressales")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "educacao")

        if db_password:
            database_url = (
                f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )
        else:
            database_url = f"postgresql+psycopg2://{db_user}@{db_host}:{db_port}/{db_name}"

    connect_args = {}
    sslmode = _get_sslmode(database_url)

    if sslmode:
        connect_args["sslmode"] = sslmode

    return create_engine(
        database_url,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
