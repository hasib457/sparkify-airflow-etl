# pylint:  disable-all

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging

class LoadDimensionOperator(BaseOperator):

    """
    LoadDimensionOperator loads data into a dimension table in Redshift from SQL commands.

    :param redshift_conn_id (str): Airflow connection ID for the Redshift database.
    :param table (str): Name of the dimension table to load data into.
    :param sql_command (str): SQL command to extract data for the dimension table.
    :param insert_mode (str): Optional string to specify the insert mode to use, either "append" (default) or "truncate".
                           If "truncate" is selected, the target table will be truncated before data is loaded.

    """

    ui_color = '#F98866'

    def __init__(self,
                redshift_conn_id: str = "redshift",
                table: str ="",
                sql_command: str = "",
                insert_mode:str ="truncate",
                *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table =table
        self.sql_command = sql_command
        self.insert_mode=insert_mode

        if self.insert_mode not in ["append", "truncate"]:
            raise ValueError("Insert mode must be either 'append' or 'truncate'.")

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.insert_mode == "truncate":
            truncate_statement = f"TRUNCATE TABLE {self.table};"
            redshift_hook.run(truncate_statement, autocommit=True)

        insert_statment = f"INSERT INTO {self.table} ({self.sql_command});"
        success = redshift_hook.run(insert_statment, autocommit=True)
        print(success)
        # if not success:
        #     raise ValueError(
        #         f"Failed to insert data in the {self.table} table with SQL statement:  { insert_statment}"
        #     )
        logging.info(f"Data inserted in {self.table} Table successfully in Redshift.")
