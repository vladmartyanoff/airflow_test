from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator



default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 10)
}

dag = DAG('amplitude_weather_dag', default_args=default_args, schedule='* * * * *', catchup=False, max_active_tasks=3, max_active_runs=1, tags=["weather", "amplitude"])

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='main_postgresql_connection',
    sql="amp_weather_sql/table_creating.sql",
    dag=dag
)

test_ping = PythonOperator(
    task_id="task1",
    python_callable=dt_check,
    dag=dag
)

confirming = PythonOperator(
    task_id="task2",
    python_callable=dt_succes,
    dag=dag
)

test_ping >> confirming