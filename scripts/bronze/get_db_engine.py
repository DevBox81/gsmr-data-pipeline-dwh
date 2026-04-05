from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

@lru_cache(maxsize=1)
def get_engine(schema) -> Engine:
    #change these variables with your own variables
    HOST = "localhost"
    PORT = 5432
    NAME = "gsmr_dwh"
    USER = "postgres"
    PASSWORD = 1330
    SHCEMA = f"{schema}"

    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}"

    connect_args={"options":f"-csearch_path={SHCEMA},public"}

    engine = create_engine (
        url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args
    )
    return engine
