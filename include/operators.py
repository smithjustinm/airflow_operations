from typing import List, TypeVar, Optional

import pandas as pd
import sqlalchemy as sq
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

import logging

logger = logging.getLogger(__name__)

TInsertDataOperator = TypeVar(
    "TInsertDataOperator", bound="InsertDataOperator"
)


class InsertDataOperator(BaseOperator):
    """Operator that inserts data from parquet files, performs transformations, and inserts the data into Postgres.

    Args:
        target_conn_id (str): The Airflow connection ID for the target database.
        parquet_file (str): The path to the parquet file to insert into Postgres.
        target_db (str): The target database in Postgres.
        target_table (str): The target table in Postgres.
        target_schema (str): The target schema in Postgres.
        if_exists (str): The behavior when the table exists. Defaults to "append".
        drop_cols (List[str]): The columns to drop from the parquet file.
        fillna_cols (List[str]): The columns to fill NaNs with zeros as floats.
        **kwargs: Additional keyword arguments to pass to the BaseOperator.
    """

    def __init__(
        self: TInsertDataOperator,
        target_conn_id: str,
        parquet_file: str,
        target_db: str,
        target_table: str,
        target_schema: str,
        if_exists: str = "append",
        drop_cols: Optional[List[str]] = None,
        fillna_cols: Optional[List[str]] = None,
        **kwargs,

    ):
        super().__init__(**kwargs)
        self.target_conn_id = target_conn_id
        self.parquet_file = parquet_file
        self.target_db = target_db
        self.target_table = target_table
        self.target_schema = target_schema
        self.if_exists = if_exists
        self.drop_cols = drop_cols
        self.fillna_cols = fillna_cols

    def perform_transformations(self) -> pd.DataFrame:
        """Performs transformations on the data.

        Args:
            data (pd.DataFrame): The data to transform.

        Returns:
            pd.DataFrame: The transformed data.
        """
        # read in the parquet file
        data = pd.read_parquet(self.parquet_file)

        # remove the columns
        if self.drop_cols is not None:
            data = data.drop(columns=self.drop_cols)

        # convert NaNs to zeros for specific columns
        if self.fillna_cols is not None:
            data[self.fillna_cols] = data[self.fillna_cols].fillna(0.0)

        return data

    def _insert_data(self, data: pd.DataFrame):
        """Inserts the data from perform_transformations into Postgres.

        Args:
            data (pd.DataFrame): The data to insert into Postgres.

        Returns:
            int: The number of rows inserted into Postgres.

        NB: to_sql can take a callable as a method, which is used to insert the data.
        """
        pg_conn = BaseHook.get_connection(conn_id=self.target_conn_id)
        engine = sq.create_engine(
            f"postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
        )

        try:
            logger.info(f"Inserting data into Postgres table: {self.target_table}")
            with engine.connect().execution_options(auto_commit=True) as conn:
                data.to_sql(
                    self.target_table,
                    conn,
                    schema=self.target_schema,
                    index=False,
                    if_exists=self.if_exists,
                )

        except Exception as e:
            logger.exception(
                {
                    "message": f"Error in inserting data into Postgres table: {self.target_table}",
                },
            )
            raise e

    def execute(self, context):
        """Executes the operator."""
        data = self.perform_transformations()
        self._insert_data(data)
