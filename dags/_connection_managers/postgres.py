""" Postgres Connection Manager DAG """
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# noinspection PyProtectedMember
from dags._connection_managers.utils import update_airflow_connection
from include.settings import Settings, Tags

settings = Settings()

# NOTE: do not adjust `dag_start_date` once a DAG has been defined.  if you need to
#       change this value, you should rename the dag_id.
#       More Info here: https://www.astronomer.io/guides/dag-best-practices.

dag_start_date = datetime(
    year=2023, month=5, day=15, tzinfo=pendulum.timezone("US/Central")
)


def generate_update_postgres_connection_operator(
    conn_id: str,
    host: str,
    port: int,
    user: str,
    password: str,
    dbname: str,
):
    """Generate a PythonOperator for updating a Postgres connection in Airflow.

    Args:
        conn_id (str): Airflow connection id.
        host (str): Postgres host.
        port (int): Postgres port.
        user (str): Postgres username.
        password (str): Postgres password.
        dbname (str): Postgres database name.

    Returns:
        PythonOperator: PythonOperator for updating a postgres connection in Airflow.
    """

    def update_postgres_connection():

        new_conn = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host=host,
            port=port,
            login=user,
            password=password,
            schema=dbname,
        )

        update_airflow_connection(new_conn=new_conn)

        # Test the connection
        conn_test = PostgresHook(postgres_conn_id=conn_id).test_connection()
        if conn_test[0] is False:
            raise Exception(conn_test[1])

    return PythonOperator(
        task_id=f"update_connection_{conn_id}",
        python_callable=update_postgres_connection,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
        trigger_rule="always",
    )


with DAG(
    dag_id="postgres_connection_updater",
    description="Update Postgres connection in Airflow",
    start_date=dag_start_date,
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=[Tags.CONNECTION_MANAGER, Tags.POSTGRES],
    is_paused_upon_creation=False,
) as dag:
    generate_update_postgres_connection_operator(
        conn_id=settings.postgres_conn_id,
        host=settings.HOST,
        port=int(settings.PORT),
        user=settings.USER,
        password=settings.PASSWORD.get_secret_value(),
        dbname=settings.DBNAME,
    )
