from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'admin',
    'start_date': pendulum.now(),
    'retries':3,
    'catchup':False,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False,
    'depends_on_past':False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=1
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="phil-airflow",
        s3_key="log-data",
        json_file="s3://phil-airflow/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='phil-airflow',
        s3_key='song-data',
        json_file='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id ='redshift',
        sql_query=SqlQueries.songplay_table_insert,
        table='fact_songplay'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id ='redshift',
        sql_query=SqlQueries.user_table_insert,
        table='dim_users',
        truncate=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id ='redshift',
        sql_query=SqlQueries.song_table_insert,
        table='dim_songs',
        truncate=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id ='redshift',
        sql_query=SqlQueries.artist_table_insert,
        table='dim_artist',
        truncate=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id ='redshift',
        sql_query=SqlQueries.time_table_insert,
        table='dim_times',
        truncate=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = 'redshift',
        tables=['dim_users','dim_songs','dim_artist','dim_times','fact_songplay']
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()