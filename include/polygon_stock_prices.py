from datetime import datetime
import time
import os
import requests
import boto3
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


def s3_catalog():
    """Returns s3 bucket catalog"""

    os.environ['AWS_ACCESS_KEY_ID'] = os.environ['DATAEXPERT_AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] =  os.environ['DATAEXPERT_AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_REGION'] = os.environ['DATAEXPERT_AWS_REGION']
    os.environ['AWS_DEFAULT_REGION'] = os.environ['DATAEXPERT_AWS_DEFAULT_REGION']
    os.environ['AWS_S3_BUCKET_TABULAR'] = os.environ['DATAEXPERT_S3_BUCKET_TABULAR']

    boto3.setup_default_session(region_name=os.environ['DATAEXPERT_AWS_REGION'])
    # Configure PyIceberg to use AWS Glue catalog
    catalog = load_catalog(
        name="glue_catalog",
        **{
            "type": "glue",
            "region": os.environ['DATAEXPERT_AWS_REGION'],
            "warehouse": f"s3://{os.environ['AWS_S3_BUCKET_TABULAR']}/iceberg-warehouse/",
        }
    )
    return catalog


def fetch_stock_prices(run_date: str, tickers_table: str, staging_table: str, polygon_api_key: str):
    """Fetch stock prices from Polygon API for tickers in the tickers table."""

    catalog = s3_catalog()
    date = datetime.strptime(run_date, "%Y-%m-%d").date()
    
    # Load tickers from the tickers table for the given date
    print(f"Loading tickers from {tickers_table} for date {date}")
    all_tickers = catalog.load_table(tickers_table)
    # Query tickers for the specific date
    tickers = all_tickers.scan(row_filter=f"date = '{date}'").to_pandas()
    
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
    
    try:
        table = catalog.load_table(staging_table)
        print(f"Table {staging_table} already exists")
    except Exception:
        spec = PartitionSpec(PartitionField(source_id=10, field_id=1000, transform=DayTransform(), name="load_date_day"))
        table = catalog.create_table(identifier=staging_table, schema=schema, partition_spec=spec)
        print(f"Created table {staging_table}")
    
    table.overwrite(pa_table)
    
    print(f"Successfully wrote {len(stock_prices)} stock price records to {staging_table} for date {date}")
    return f"Successfully wrote {len(stock_prices)} stock price records to {staging_table} for date {date}"


def dq_stock_prices(staging_table: str, run_date: str):
    """Data Quality Check for Polygon API data"""
    print(f"Running DQ on {staging_table} for {run_date}")
    catalog = s3_catalog()
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


def promote_stock_prices(staging_table: str, production_table: str, run_date: str):
    """Promote staged Polygon API data to Production"""
    
    print(f"Promoting {staging_table} -> {production_table} for {run_date}")
    catalog = s3_catalog()
    source = catalog.load_table(staging_table)
    target = catalog.load_table(production_table)

    run_dt = datetime.strptime(run_date, "%Y-%m-%d").isoformat()
    partition = source.scan(row_filter=f"load_date = '{run_dt}'").to_arrow()
    target.overwrite(partition, overwrite_filter=f"load_date >= '{run_dt}'")


def tidyup_stock_prices(staging_table: str, run_date: str):
    """TidyUp Polygon API data Staging"""

    print(f"Cleaning up staging table {staging_table} for {run_date}")
    catalog = s3_catalog()
    staging = catalog.load_table(staging_table)
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").isoformat()
    staging.delete(delete_filter=f"load_date = '{run_dt}'")
