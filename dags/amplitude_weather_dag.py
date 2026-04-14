from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 10),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG('amplitude_weather_dag', default_args=default_args, schedule='30 23 * * *', catchup=False, max_active_tasks=3, max_active_runs=1, tags=["weather", "amplitude"], description="Daily max temperature per city")

create_table = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='main_postgresql_connection',
    sql="amp_weather_sql/table_creating.sql",
    dag=dag
)

amp_weather_insert = SQLExecuteQueryOperator(
    task_id='amp_weather_insert',
    conn_id='main_postgresql_connection',
    sql="amp_weather_sql/amp_weather_insert.sql",
    dag=dag
)

confirming = SQLExecuteQueryOperator(
    task_id='confirming',
    conn_id='main_postgresql_connection',
    sql="amp_weather_sql/task-completed.sql",
    dag=dag
)

create_table >> amp_weather_insert >> confirming