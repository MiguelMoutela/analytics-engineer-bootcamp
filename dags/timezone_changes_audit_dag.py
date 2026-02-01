import os
from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from include import postgres_to_snowflake, dataexpert_snowflake as snowflake

@dag(
    description="DAG to audit timezone changes from DataExpert.io students",
    default_args={
        "owner": os.environ['DATAEXPERT_DAG_OWNER'],
        "start_date": datetime(2026, 2, 1),
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

    fetch_users = PythonOperator(
        task_id="fetch_users",
        python_callable=postgres_to_snowflake.migrate,
        op_kwargs={
            "source": "student_api.users",
            "target": ".".join([
                os.environ["SF_DATABASE"],
                os.environ["SF_SCHEMA"],
                "USER_TIMEZONE_RAW"
            ])
        }
    )

    fetch_timezone_changes = PythonOperator(
        task_id="fetch_timezone_changes",
        python_callable=postgres_to_snowflake.migrate,
        op_kwargs={
            "source": "student_api.timezone_audit_tracking",
            "target": ".".join([
                os.environ["SF_DATABASE"],
                os.environ["SF_SCHEMA"],
                "USER_TIMEZONE_AUDIT_RAW"
            ])
        }
    )

    check_source_table = PythonSensor(
        task_id="check_source_table",
        python_callable=snowflake.has_table,
        poke_interval=60, 
        timeout=7200,      
        mode='reschedule',
        op_kwargs={
            "table": "USER_TIMEZONE_AUDIT_RAW"
        }
    )

    build_scd2 = PythonOperator(
        task_id="build_scd2",
        python_callable=snowflake.create_view_user_timezone_scd2
    )

    fetch_users >> fetch_timezone_changes >> check_source_table >> build_scd2

timezone_changes_audit_dag()
