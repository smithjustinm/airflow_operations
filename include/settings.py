import os
from enum import Enum

from pydantic import BaseSettings, SecretStr

APP_NAME = "airflow-local"


class Settings(BaseSettings):
    """Pydantic setting management class for default configs"""

    app_name: str = APP_NAME
    log_level: str = "INFO"

    owner: str = "airflow"
    email_on_failure: bool = False
    email_on_retry: bool = False

    # Postgres configs
    postgres_conn_id: str = "postgres"
    HOST: str = os.getenv("host")
    PORT: str = os.getenv("port")
    USER: str = os.getenv("user")
    PASSWORD: SecretStr = os.getenv("password")
    DBNAME: str = os.getenv("dbname")


class Tags(str, Enum):
    CONNECTION_MANAGER: str = "connection_manager"
    POSTGRES: str = "Postgres"
