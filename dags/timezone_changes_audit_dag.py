import os
from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from include.postgres_queries import execute as postgres_execute

@dag(
    description="DAG to audit timezone changes from DataExpert.io students",
    default_args={
        "owner": os.environ['DATAEXPERT_DAG_OWNER'],
        "start_date": datetime(2026, 1, 14),
        "end_date": datetime(2026, 1, 14),
        "retries": 1,
        "execution_timeout": timedelta(hours=2),
    },
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
    tags=["community", "dataexpert.io", "students", "timezone", "snowflake", "postgres"],
)
def timezone_changes_audit_dag():
    """Processes timezone changes from DataExpert.io"""

    SRC_STUDENTS_TABLE = "student_api.users"
    SRC_STUDENT_TIMEZONES_TABLE = "student_api.timezone_audit_tracking"

    # wait_for_changes = PythonSensor(
    #     task_id="wait_timezone_changes",
    #     python_callable=postgres_execute,
    #     poke_interval=60, 
    #     timeout=7200,      
    #     mode='reschedule',
    #     op_kwargs={
    #         "query": "SELECT 1;"
    #     }
    # )

    fetch_timezone_changes = PythonOperator(
        task_id="fetch_timezone_changes",
        python_callable=postgres_execute,
        op_kwargs={
            "run_from_date": "{{ ds }}",
            "run_to_date": "{{ ds }}",
            "student_table": SRC_STUDENTS_TABLE,
            "student_timezone_table": SRC_STUDENT_TIMEZONES_TABLE
        }
    )

    fetch_timezone_changes

timezone_changes_audit_dag()
