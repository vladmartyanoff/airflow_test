from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dag1_scripts.task1 import dt_check
from dag1_scripts.task2 import dt_succes


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 28)
}

dag = DAG('dag1', default_args=default_args, schedule='* * * * *', catchup=False, max_active_tasks=3, max_active_runs=1, tags=["Test", "First_DAG"])

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