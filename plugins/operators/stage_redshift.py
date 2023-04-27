# pylint:  disable-all

import logging

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    """
    Copies data from an S3 bucket to a Redshift table using the COPY command.

    :param table: The name of the target Redshift table.
    :type table: str
    :param s3_bucket: The name of the source S3 bucket.
    :type s3_bucket: str
    :param s3_key: The key of the source S3 object.
    :type s3_key: str
    :param aws_conn_id: The Airflow connection ID for AWS.
    :type aws_conn_id: str
    :param redshift_conn_id: The Airflow connection ID for Redshift.
    :type redshift_conn_id: str
    :param json_path: json format path
    :type: str
    :param delimiter: delimiter type
    :type: str
    :param copy_options: List of strings containing additional COPY options.
    :type copy_options: list
    """

    ui_color = "#358140"
    template_fields = ("s3_key", "json_path")

    def __init__(
        self,
        # Define operators params
        redshift_conn_id: str = "redshift",
        aws_conn_id: str = "aws",
        s3_key: str = "",
        s3_bucket: str = "",
        s3_region: str = "",
        table: str = "",
        format: str = "json",
        json_path: str = "",
        delimiter: str = "",
        copy_options: list = [],
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.table = table
        self.format = format
        self.json_path = json_path
        self.delimiter = delimiter
        self.copy_options = " ".join(copy_options)

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear stage table
        self.log.info(f"Clearing {self.table} stage table")
        redshift.run(f"DELETE FROM {self.table}")

        # aws credintilas
        access_key = s3_hook.get_credentials().access_key
        secret_key = s3_hook.get_credentials().secret_key

        copy_sql = """
            COPY {}
            FROM '{}'
            CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
            {}
            REGION '{}'
            ;
        """

        self.s3_key = self.s3_key.format(**context)
        s3_url = f"s3://{self.s3_bucket}/{self.s3_key}"

        logging.info(f"- S3 URL: {s3_url}")

        # Selecting format
        if self.format == "csv":
            autoformat = f"DELIMITER '{self.delimiter}'"
        elif self.format == "json":
            jsonoption = self.json_path or "auto"
            autoformat = f"FORMAT AS JSON '{jsonoption}'"

        if self.copy_options:
            copy_sql += f" {self.json_path}"

        formated_copy_sql = copy_sql.format(
            self.table, s3_url, access_key, secret_key, autoformat, self.s3_region
        )

        logging.info("Copying data from S3 to Redshift")

        redshift.run(formated_copy_sql)
