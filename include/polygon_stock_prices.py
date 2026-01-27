import sys
from datetime import datetime, timedelta
import requests
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    TimestampType,
    DoubleType,
    LongType,
    NestedField
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import os
import boto3
from dotenv import load_dotenv
import time


def _setup_aws_creds(aws_access_key_id: str, aws_secret_access_key: str, aws_region: str, s3_bucket: str):
    """Sets AWS credentials"""
    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    os.environ['AWS_REGION'] = aws_region
    os.environ['AWS_DEFAULT_REGION'] = aws_region
    os.environ['AWS_S3_BUCKET_TABULAR'] = s3_bucket

def s3_catalog(
    aws_access_key_id: str, 
    aws_secret_access_key: str, 
    aws_region: str = 'us-west-2', 
    s3_bucket: str = 'zachwilsonsorganization-522'
):
    """Returns s3bucket catalog"""
    _setup_aws_creds(aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket)
    aws_region = os.environ.get('AWS_REGION')
    s3_bucket = os.environ.get('AWS_S3_BUCKET_TABULAR')
    boto3.setup_default_session(region_name=aws_region)
    # Configure PyIceberg to use AWS Glue catalog
    catalog = load_catalog(
        name="glue_catalog",
        **{
            "type": "glue",
            "region": aws_region,
            "warehouse": f"s3://{s3_bucket}/iceberg-warehouse/",
        }
    )
    
    return catalog


def fetch_stock_prices(
    run_date: str,
    tickers_table: str = "zachwilson.polygon_tickers",
    staging_table: str = "zachwilson.polygon_stock_prices",
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    polygon_api_key: str = None,
):
    """
    Fetch stock prices from Polygon API for tickers in the tickers table.
    
    Args:
        run_date: Date string in YYYY-MM-DD format
        tickers_table: Source Iceberg table with ticker list
        staging_table: Target Iceberg table for stock prices
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        aws_region: AWS region
        s3_bucket: S3 bucket for Iceberg warehouse
    """
    catalog = s3_catalog(aws_access_key_id, aws_secret_access_key)
    # Parse run date
    date = datetime.strptime(run_date, "%Y-%m-%d").date()
    
    # Load tickers from the tickers table for the given date
    print(f"Loading tickers from {tickers_table} for date {date}")
    all_tickers = catalog.load_table(tickers_table)
    
    # Query tickers for the specific date
    tickers = all_tickers.scan(
        row_filter=f"date = '{date}'"
    ).to_pandas()
    
    if tickers.empty:
        raise ValueError(f"No tickers found for date {date}")
    
    ticker_symbols = set(tickers['ticker'].dropna())
    print(f"Found {len(ticker_symbols)} tickers to fetch prices for")

    locale_market_pairs = tickers[["locale", "market"]].dropna().drop_duplicates()
    
    # Fetch stock prices from Polygon API
    stock_prices = []
    
    for row in locale_market_pairs.itertuples(index=False):
        agg_url = f"https://api.polygon.io/v2/aggs/grouped/locale/{row.locale}/market/{row.market}/{run_date}?apiKey={polygon_api_key}"
        try:
            print(f"Calling: locale={row.locale}, market={row.market}")
            response = requests.get(agg_url)
            data = response.json()

            if data.get('status') == 'OK' and data.get('results'):
                for result in data['results']:
                    if result["T"] in ticker_symbols:
                        stock_prices.append({
                            'ticker': result["T"],
                            'timestamp': datetime.fromtimestamp(result['t'] / 1000),
                            'open': result.get('o'),
                            'high': result.get('h'),
                            'low': result.get('l'),
                            'close': result.get('c'),
                            'volume': result.get('v'),
                            'vwap': result.get('vw'),
                            'transactions': result.get('n'),
                            'load_date': datetime.combine(date, datetime.min.time()),
                        })
                        ticker_symbols.discard(result["T"])
                    else:
                        print(f"Skipped out-of-scope ticker {result['T']}")
                # Rate limiting - Polygon free tier allows 5 requests per minute
                time.sleep(0.2)
            
        except Exception as e:
            print(f"Error fetching data for {row.locale}, {row.market}: {str(e)}")
            continue
    
    if not stock_prices:
        raise ValueError(f"No stock prices fetched for date {date}")
    
    print(f"Fetched {len(stock_prices)} stock price records")
    
    # Define PyIceberg schema for stock prices
    schema = Schema(
        NestedField(1, "ticker", StringType(), required=False),
        NestedField(2, "timestamp", TimestampType(), required=False),
        NestedField(3, "open", DoubleType(), required=False),
        NestedField(4, "high", DoubleType(), required=False),
        NestedField(5, "low", DoubleType(), required=False),
        NestedField(6, "close", DoubleType(), required=False),
        NestedField(7, "volume", LongType(), required=False),
        NestedField(8, "vwap", DoubleType(), required=False),
        NestedField(9, "transactions", LongType(), required=False),
        NestedField(10, "load_date", TimestampType(), required=False)
    )
    
    # Create PyArrow schema
    pa_schema = pa.schema([
        ('ticker', pa.string()),
        ('timestamp', pa.timestamp('us')),
        ('open', pa.float64()),
        ('high', pa.float64()),
        ('low', pa.float64()),
        ('close', pa.float64()),
        ('volume', pa.int64()),
        ('vwap', pa.float64()),
        ('transactions', pa.int64()),
        ('load_date', pa.timestamp('us'))
    ])
    
    # Convert to PyArrow table
    pa_table = pa.Table.from_pylist(stock_prices, schema=pa_schema)
    
    # Create or load table
    namespace, table_name = staging_table.split('.')
    try:
        table = catalog.load_table(staging_table)
        print(f"Table {staging_table} already exists")
    except Exception:
        # Create table if it doesn't exist
        partition_spec = PartitionSpec(
            PartitionField(source_id=10, field_id=1000, transform=DayTransform(), name="load_date_day")
        )
        
        table = catalog.create_table(
            identifier=staging_table,
            schema=schema,
            partition_spec=partition_spec
        )
        print(f"Created table {staging_table}")
    
    # Overwrite partition for the run_date
    table.overwrite(pa_table)
    
    print(f"Successfully wrote {len(stock_prices)} stock price records to {staging_table} for date {date}")
    return f"Successfully wrote {len(stock_prices)} stock price records to {staging_table} for date {date}"


def dq_stock_prices(staging_table: str, run_date: str, aws_access_key_id: str, aws_secret_access_key: str):
    """Data Quality Check for Polygon API data"""
    print(f"Running DQ on {staging_table} for {run_date}")
    catalog = s3_catalog(aws_access_key_id, aws_secret_access_key)
    staging = catalog.load_table(staging_table)
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").isoformat()
    partition = staging.scan(row_filter=f"load_date = '{run_dt}'").to_pandas()

    has_unique_tickers = partition["ticker"].is_unique
    has_valid_tickers = partition["ticker"].notna().all()
    has_valid_volume = (partition["volume"] >= 0).all()
    has_valid_timestamp = partition["timestamp"].notna().all()
    has_valid_price_data = (partition["open"] > 0).all() and (partition["close"] > 0).all() and (partition["high"] > 0).all() and (partition["low"] > 0).all()

    is_valid = has_unique_tickers and has_valid_tickers and has_valid_volume and has_valid_timestamp and has_valid_price_data
    if not is_valid:
        failed_rules = [
            msg for msg, passed in [
                ("ticker set contains duplicates", has_unique_tickers),
                ("ticker set contains nulls", has_valid_tickers),
                ("stocks show invalid volume figure", has_valid_volume),
                ("timestamp entries missing", has_valid_timestamp),
                ("stocks show invalid price data", has_valid_price_data)
            ] if not passed
        ]
        raise ValueError(f"DQ step failed for the following rules: {' and '.join(failed_rules)}")


def promote_stock_prices(staging_table: str, production_table: str, run_date: str, aws_access_key_id: str, aws_secret_access_key: str):
    """Promote staged Polygon API data to Production"""
    print(f"Promoting {staging_table} -> {production_table} for {run_date}")
    catalog = s3_catalog(aws_access_key_id, aws_secret_access_key)
    source = catalog.load_table(staging_table)
    target = catalog.load_table(production_table)

    run_dt = datetime.strptime(run_date, "%Y-%m-%d").isoformat()
    partition = source.scan(row_filter=f"load_date = '{run_dt}'").to_arrow()
    
    upsert_result = target.upsert(partition, join_cols=["ticker","load_date"])


def tidyup_stock_prices(staging_table: str, run_date: str, aws_access_key_id: str, aws_secret_access_key: str):
    """TidyUp Polygon API data Staging"""
    print(f"Cleaning up staging table {staging_table} for {run_date}")
    catalog = s3_catalog(aws_access_key_id, aws_secret_access_key)
    staging = catalog.load_table(staging_table)
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").isoformat()
    staging.delete(delete_filter=f"load_date = '{run_dt}'")
    

# Script execution mode (when run directly)
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Get arguments from command line
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    tickers_table = sys.argv[2] if len(sys.argv) > 2 else "zachwilson.polygon_tickers"
    staging_table = sys.argv[3] if len(sys.argv) > 3 else "zachwilson.polygon_stock_prices"
    
    fetch_stock_prices(run_date, tickers_table, staging_table)
