from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.standard.sensors.python import PythonSensor
from include.eczachly.scripts.polygon_stock_prices import fetch_stock_prices, dq_stock_prices, promote_stock_prices, tidyup_stock_prices
from include.eczachly.trino_queries import execute_trino_query

def run_stock_prices_script(**context):
    """Execute the stock prices fetching function with AWS credentials"""
    
    result = fetch_stock_prices(
        run_date=context['ds'],
        tickers_table="miguelmoutela.stock_tickers",
        staging_table="miguelmoutela.stock_prices_staging",
        aws_access_key_id=Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY"),
        polygon_api_key=Variable.get("POLYGON_API_KEY")
    )

    return result

@dag(
    description="A DAG that waits for Polygon ticker data then fetches stock prices and creates Snowflake Iceberg table",
    default_args={
        "owner": "Miguel Almeida",
        "start_date": datetime(2026, 1, 11),
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

    wait_for_tickers = PythonSensor(
        task_id="wait_for_polygon_tickers",
        python_callable=execute_trino_query,
        poke_interval=60, 
        timeout=7200,      
        mode='reschedule',
        op_kwargs={
            "query": """
                SELECT count(*) > 0 FROM miguelmoutela.stock_tickers WHERE date=DATE('{{ ds }}')
            """,
            "aws_access_key_id": Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
        }
    )

    fetch_prices = PythonOperator(
        task_id='fetch_stock_prices',
        python_callable=run_stock_prices_script
    )

    dq_staged_stock_prices_step = PythonOperator(
        task_id="dq_stock_prices",
        python_callable=dq_stock_prices,
        op_kwargs={
            "staging_table": 'miguelmoutela.stock_prices_staging',
            "run_date": "{{ ds }}",
            "aws_access_key_id": Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY"),
        }
    )

    promote_stock_prices_step = PythonOperator(
        task_id="promote_stock_prices",
        python_callable=promote_stock_prices,
        op_kwargs={
            "staging_table": 'miguelmoutela.stock_prices_staging',
            "production_table": 'miguelmoutela.stock_prices',
            "run_date": "{{ ds }}",
            "aws_access_key_id": Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY"),
        }
    )

    tidyup_stock_prices_step = PythonOperator(
        task_id="tidyup_stock_prices",
        python_callable=tidyup_stock_prices,
        op_kwargs={
            "staging_table": 'miguelmoutela.stock_prices_staging',
            "run_date": "{{ ds }}",
            "aws_access_key_id": Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY"),
        }
    )

    wait_for_tickers >> fetch_prices >> dq_staged_stock_prices_step >> promote_stock_prices_step >> tidyup_stock_prices_step

massive_stock_prices_dag()
