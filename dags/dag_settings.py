from datetime import datetime

import pendulum
from airflow.models import Connection

from include.settings import Settings


class DagSettings(Settings):
    """Settings for the DAG."""

    _pg_conn = None

    def get_pg_conn(self):
        return Connection.get_connection_from_secrets(self.postgres_conn_id)

    @property
    def dag_start_date(self):
        """DAG start date."""
        return datetime(year=2023, month=5, day=15, tzinfo=pendulum.timezone("US/Central"))
