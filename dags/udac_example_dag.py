from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

# /opt/airflow/start.sh

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default':False 
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs =1
          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#create_all_tables  = PostgresOperator(
#  task_id="create_tables",
#  dag=dag,
#  sql='create_tables.sql',
#  postgres_conn_id="redshift"
#)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshiftConnId="redshift",
    awsCredentialsId="aws_credentials",
    table="staging_events",
    s3Bucket="udacity-dend",
    s3Key="log_data/",
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshiftConnId = "redshift",
    awsCredentialsId = "aws_credentials",
    table="staging_songs",
    s3Bucket="udacity-dend",
    s3Key="song_data/",
    region="us-west-2",
    json="auto",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshiftConnId="redshift",
    sqlStatement=SqlQueries.songplay_table_insert,
    tableName = "songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshiftConnId="redshift",
    sqlStatement=SqlQueries.user_table_insert,
    isIncremental=True,
    tableName = "users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshiftConnId="redshift",
    sqlStatement=SqlQueries.song_table_insert,
    isIncremental=True,
    tableName = "songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshiftConnId="redshift",
    sqlStatement=SqlQueries.artist_table_insert,
    isIncremental=True,
    tableName = "artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshiftConnId="redshift",
    sqlStatement=SqlQueries.time_table_insert,
    isIncremental=True,
    tableName = "times"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshiftConnId="redshift",
    dataQualityCheck=[
        {'outputSQL': "SELECT COUNT(*) FROM users WHERE userid is null", 'acceptedValue': 0},{'outputSQL': "SELECT COUNT(*) FROM users WHERE userid is null", 'acceptedValue': 0}
    ]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]

[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]

[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
