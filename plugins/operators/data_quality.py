# pylint:  disable-all

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    """
    Performs data quality checks on a set of tables in Redshift.

    :param dq_checks: List check statments for data quality
    :type dq_checks: list[str]
    :param redshift_conn_id: ID of the Redshift connection to use
    :type redshift_conn_id: str
    """

    ui_color = "#89DA59"

    def __init__(
        self,
        dq_checks: list = [],
        redshift_conn_id: str = "redshift",
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for i, dq_check in enumerate(self.dq_checks):
            test_sql = dq_check["test_sql"]
            expected_result = dq_check["expected_result"]
            comparison = dq_check.get("comparison", "=")

            records = redshift_hook.get_records(test_sql)
            result = records[0][0]

            if comparison == "=":
                if result != expected_result:
                    raise ValueError(
                        f"Data quality check #{i} failed. Test query {test_sql} returned {result}, expected {expected_result}"
                    )
            elif comparison == ">":
                if result <= expected_result:
                    raise ValueError(
                        f"Data quality check #{i} failed. Test query {test_sql} returned {result}, expected greater than {expected_result}"
                    )
            elif comparison == "<":
                if result >= expected_result:
                    raise ValueError(
                        f"Data quality check #{i} failed. Test query {test_sql} returned {result}, expected less than {expected_result}"
                    )
            else:
                raise ValueError(
                    f"Invalid comparison operator '{comparison}' for data quality check #{i}."
                )
