# pylint:  disable-all

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    """
    Performs data quality checks on a set of tables in Redshift.

    :param tables: List of tables to check for data quality
    :type tables: list[str]
    :param redshift_conn_id: ID of the Redshift connection to use
    :type redshift_conn_id: str
    """

    ui_color = "#89DA59"

    def __init__(
        self,
        redshift_conn_id: str | None = "redshift",
        tables: list | None = [],
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            # Check that the table exists
            table_exists = redshift_hook.run(f"SELECT 1 FROM {table} LIMIT 1")
            if not table_exists:
                raise ValueError(f"Data quality check failed. {table} does not exist")

            # Check that the table has rows
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )
