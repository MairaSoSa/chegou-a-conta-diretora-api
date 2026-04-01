import os
from sqlalchemy import create_engine

DATABASE_URL = (
    os.getenv("RAILWAY_DATABASE_URL")
    or os.getenv("DATABASE_URL")
    or os.getenv("DATABASE_PUBLIC_URL")
)

if not DATABASE_URL:
    raise ValueError(
        "Defina DATABASE_URL ou DATABASE_PUBLIC_URL para conectar no banco PostgreSQL."
    )

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

connect_args = {}

db_sslmode = os.getenv("DB_SSLMODE")
if db_sslmode:
    connect_args["sslmode"] = db_sslmode
elif "proxy.rlwy.net" in DATABASE_URL:
    connect_args["sslmode"] = "require"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=connect_args,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=300,
)
