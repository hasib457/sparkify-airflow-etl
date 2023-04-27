# pylint:  disable-all

import logging

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CreateRedshiftTableOperator(BaseOperator):
    """
    Operator that creates a Redshift table by executing a SQL statement.

    :param redshift_conn_id: The connection ID for Redshift.
    :type redshift_conn_id: str
    :param sql: The SQL statement to execute for creating a table.
    :type sql: list
    """

    ui_color = "#C98066"

    def __init__(
        self,
        redshift_conn_id: str = "redshift",
        sql: list = [],
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.sql:
            success = redshift_hook.run(table)
            print(success)
            if not success:
                # raise ValueError(
                #     f"Failed to create table with SQL statement:  { table}"
                # )
                logging.info(f"Table {table} created successfully in Redshift.")
