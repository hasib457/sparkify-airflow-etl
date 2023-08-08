import unittest
from unittest import mock
from airflow.models import DagBag

from operators import (
    CreateRedshiftTableOperator,
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
)

from dags import sparkify_dag

class TestSparkifyDAG(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag()
        self.dag_id = sparkify_dag.dag_id
        self.dag = self.dagbag.get_dag(self.dag_id)
        self.tasks = self.dag.tasks
        self.default_args = sparkify_dag.default_args

    def test_dag_loaded(self):
        self.assertGreater(len(self.dagbag.import_errors), 0)
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 8)

    def test_operators(self):
        operators = [task.__class__ for task in self.tasks]
        self.assertIn(StageToRedshiftOperator, operators)
        self.assertIn(LoadFactOperator, operators)
        self.assertIn(LoadDimensionOperator, operators)
        self.assertIn(DataQualityOperator, operators)

    def test_dependencies_of_load_songplays_table_task(self):
        load_songplays_table_task = self.dag.get_task('Load_songplays_fact_table')

        upstream_task_ids = list(map(lambda task: task.task_id, load_songplays_table_task.upstream_list))
        self.assertListEqual(upstream_task_ids, ['Stage_events', 'Stage_songs'])

        downstream_task_ids = list(map(lambda task: task.task_id, load_songplays_table_task.downstream_list))
        self.assertListEqual(downstream_task_ids, ['Load_user_dim_table', 'Load_song_dim_table', 'Load_artist_dim_table', 'Load_time_dim_table'])

if __name__ == '__main__':
    unittest.main()