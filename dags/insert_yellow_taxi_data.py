from airflow import DAG
from pathlib import Path

from dags.dag_settings import DagSettings
from include.operators import InsertDataOperator
from include.settings import Tags

import logging

logger = logging.getLogger(__name__)

settings = DagSettings()


def generate_insert_yellow_taxi_data_operator(file: str):
    """Generate a InsertDataOperator for inserting yellow taxi data into Postgres.

    Args:
        file (str): The path to the parquet file to insert into Postgres.

    Returns:
        InsertDataOperator: InsertDataOperator for inserting yellow taxi data into Postgres.
    """
    return InsertDataOperator(
        task_id=f"insert_yellow_taxi_data_{Path(parquet_file).stem}",
        target_conn_id=settings.postgres_conn_id,
        parquet_file=file,
        target_db="postgres",
        target_table="yellow_taxi",
        target_schema="public",
        if_exists="append",
        drop_cols=["rate_code_id", "store_and_fwd_flag"],
        fillna_cols=["congestion_surcharge", "improvement_surcharge", "airport_fee"],
        dag=dag,
    )


with DAG(
    "insert_yellow_taxi_data",
    description="Insert yellow taxi data into Postgres.",
    start_date=settings.dag_start_date,
    tags=[Tags.CONNECTION_MANAGER, Tags.POSTGRES],
    schedule_interval="@once",
) as dag:
    try:
        yellow_taxi_data_dir = Path(__file__).parent / "data"
        for parquet_file in yellow_taxi_data_dir.glob("*.parquet"):
            generate_insert_yellow_taxi_data_operator(str(parquet_file))
    except Exception as e:
        logger.exception("Error generating InsertDataOperator.", exc_info=e)
        raise e

