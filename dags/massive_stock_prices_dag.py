import os
from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from include.polygon_stock_prices import fetch_stock_prices, dq_stock_prices, promote_stock_prices, tidyup_stock_prices
from include.trino_queries import run_trino_query_dq_check

@dag(
    description="DAG to wait for Polygon ticker data and fetche stock prices and create Snowflake Iceberg table",
    default_args={
        "owner": os.environ['DATAEXPERT_DAG_OWNER'],
        "start_date": datetime(2026, 1, 14),
        "retries": 1,
        "execution_timeout": timedelta(hours=2),
    },
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
    tags=["community", "polygon", "iceberg", "stock-prices", "snowflake"],
)
def massive_stock_prices_dag():
    """Processes polygon stock price api data"""

    TICKERS_TABLE = f"{os.environ['STUDENT_SCHEMA']}.stock_tickers"
    STAGING_TABLE = f"{os.environ['STUDENT_SCHEMA']}.stock_prices_staging"
    PRODUCTION_TABLE = f"{os.environ['STUDENT_SCHEMA']}.stock_prices"

    wait_for_tickers = PythonSensor(
        task_id="wait_for_polygon_tickers",
        python_callable=run_trino_query_dq_check,
        poke_interval=60, 
        timeout=7200,      
        mode='reschedule',
        op_kwargs={
            "query": "SELECT count(*) > 0 FROM " + TICKERS_TABLE + " WHERE date=DATE('{{ ds }}')"
        }
    )

    fetch_prices = PythonOperator(
        task_id='fetch_stock_prices',
        python_callable=fetch_stock_prices,
        op_kwargs={
            "run_date": "{{ ds }}",
            "tickers_table": TICKERS_TABLE,
            "staging_table": STAGING_TABLE,
            "polygon_api_key": os.environ["DATAEXPERT_POLYGON_API_KEY"]
        }
    )

    dq_staged_stock_prices_step = PythonOperator(
        task_id="dq_stock_prices",
        python_callable=dq_stock_prices,
        op_kwargs={ "staging_table": STAGING_TABLE, "run_date": "{{ ds }}" }
    )

    promote_stock_prices_step = PythonOperator(
        task_id="promote_stock_prices",
        python_callable=promote_stock_prices,
        op_kwargs={ "staging_table": STAGING_TABLE, "production_table": PRODUCTION_TABLE, "run_date": "{{ ds }}" } 
    )

    tidyup_stock_prices_step = PythonOperator(
        task_id="tidyup_stock_prices",
        python_callable=tidyup_stock_prices,
        op_kwargs={ "staging_table": STAGING_TABLE, "run_date": "{{ ds }}" }
    )

    wait_for_tickers >> fetch_prices >> dq_staged_stock_prices_step >> promote_stock_prices_step >> tidyup_stock_prices_step

massive_stock_prices_dag()
