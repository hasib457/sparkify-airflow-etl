# pylint:  disable-all

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from helpers import SqlQueries
from operators import (
    CreateRedshiftTableOperator,
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "Hassib",
    "depends_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

dag = DAG(
    "sparkify_etl",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule="0 * * * *",
)

start_operator = EmptyOperator(task_id="Begin_execution", dag=dag)


create_tables = [
    SqlQueries.artist_table_create,
    SqlQueries.songplays_table_create,
    SqlQueries.songs_table_create,
    SqlQueries.staging_events_table_create,
    SqlQueries.staging_songs_table_create,
    SqlQueries.users_table_create,
]
create_tables = CreateRedshiftTableOperator(
    task_id="create_tables", dag=dag, redshift_conn_id="redshift", sql=create_tables
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{execution_date.year}-{execution_date.month}-0{execution_date.day}-events.json",
    s3_region="us-west-2",
    format="json",
    json_path="s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    s3_region="us-west-2",
    format="json",
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_command=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_command=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_command=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_command=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_command=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "artists", "songs", "users", "time"],
)

end_operator = EmptyOperator(task_id="Stop_execution", dag=dag)

# workflow
start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
]

[
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks

run_quality_checks >> end_operator
