import os
import requests
import tempfile
import hashlib
from pathlib import Path
from typing import List, Dict
from sec_api import QueryApi, PdfGeneratorApi, RenderApi
from dataexpert_snowflake import get_snowpark_session
import datetime


# SEC API configuration
SEC_API_KEY = os.getenv("SEC_API_KEY")

pdfApi = PdfGeneratorApi(SEC_API_KEY)
def calculate_file_checksum(file_path: str) -> str:
    """
    Calculate SHA256 checksum of a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Hexadecimal checksum string
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read file in chunks to handle large files efficiently
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def fetch_sec_10k_filings(ticker: str, limit: int = 10) -> List[Dict]:
    """
    Fetch 10-K filings using sec-api.io.
    
    Args:
        ticker: Stock ticker symbol
        limit: Maximum number of filings to fetch
    
    Returns:
        List of filing metadata dictionaries
    """
    try:
        query_api = QueryApi(api_key=SEC_API_KEY)
        
        # Query for 10-K filings for the given ticker
        query = {
            "query": f'ticker:{ticker} AND formType:"10-K"',
            "from": "0",
            "size": str(limit),
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        
        print(f"Querying sec-api.io for {ticker} 10-K filings...")
        response = query_api.get_filings(query)
        
        filings = response.get('filings', [])
        print(f"Found {len(filings)} 10-K filings for {ticker}")
        
        return filings
    except Exception as e:
        print(f"Error fetching SEC filings for {ticker}: {e}")
        return []


def download_filing_document(filing: Dict, temp_dir: Path, download_pdf: bool = True) -> str:
    """
    Download a 10-K filing document using sec-api.io.
    
    Args:
        filing: Filing metadata from sec-api
        temp_dir: Temporary directory to save files
        download_pdf: If True, download PDF; if False, download text
    
    Returns:
        Path to downloaded file
    """
    try:
        accession_number = filing.get('accessionNo', '').replace('-', '')
        ticker = filing.get('ticker', 'UNKNOWN')
        filing_date = filing.get('filedAt', '')[:10]  # Get YYYY-MM-DD
        
        if download_pdf:
            # Use PdfGeneratorApi to generate proper PDF
            filing_url = filing.get('linkToFilingDetails')
            
            print(f"Generating PDF for {ticker} filing {accession_number}...")
            # Generate PDF using PdfGeneratorApi
            content = pdfApi.get_pdf(filing_url)

            # Save PDF file
            file_path = temp_dir / f"{ticker}_{filing_date}_{accession_number}.pdf"
            file_path.write_bytes(content)
            print(f"Generated PDF: {file_path}")
            return str(file_path)
        else:
            # Download text version directly from SEC
            filing_url = filing.get('linkToTxt')
            if not filing_url:
                print(f"No text link available for {accession_number}")
                return None
            
            print(f"Downloading text for {ticker} filing {accession_number}...")
            response = requests.get(filing_url)
            response.raise_for_status()
            
            # Save text file
            file_path = temp_dir / f"{ticker}_{filing_date}_{accession_number}.txt"
            file_path.write_bytes(response.content)
            print(f"Downloaded text: {file_path}")
            return str(file_path)
            
    except Exception as e:
        print(f"Error downloading filing {filing.get('accessionNo', 'unknown')}: {e}")
        return None


def load_sec_filings_to_snowflake(
    tickers: List[str] = None, 
    schema: str = None,
    limit_per_ticker: int = 5,
    download_pdf: bool = True
):
    """
    Load SEC 10-K filings and PDFs into a Snowflake stage using Snowpark.
    
    Args:
        tickers: List of stock ticker symbols to fetch filings for
        schema: Snowflake schema to use (defaults to SCHEMA env var)
        limit_per_ticker: Number of recent filings to fetch per ticker
        download_pdf: If True, download PDFs; if False, download text files
    """
    # Get schema from environment if not provided
    if schema is None:
        schema = os.getenv("STUDENT_SCHEMA") or os.getenv("SCHEMA")
    
    # Default tickers if none provided
    if tickers is None:
        tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    
    # Get Snowpark session
    session = get_snowpark_session(schema=schema)
    print(f"Connected to Snowflake with schema: {schema}")
    
    # Create stage for SEC filings
    stage_name = f"{schema}.sec_filings_stage"
    create_stage_sql = f"""
    CREATE STAGE IF NOT EXISTS {stage_name}
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for SEC 10-K filings and documents'
    """
    
    print(f"Creating stage: {stage_name}")
    session.sql(create_stage_sql).collect()
    
    # Create table to track filings metadata
    table_name = f"{schema}.sec_filings_metadata"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        ticker VARCHAR,
        cik VARCHAR,
        accession_number VARCHAR,
        filing_date DATE,
        report_date DATE,
        form_type VARCHAR,
        file_path VARCHAR,
        file_checksum VARCHAR,
        document_url VARCHAR,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """
    
    print(f"Creating metadata table: {table_name}")
    session.sql(create_table_sql).collect()
    
    # Create temporary directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            temp_path = Path(temp_dir)
            all_filings = []
            
            try:
                # Fetch and download filings for each ticker
                for ticker in tickers:
                    print(f"\n{'='*60}")
                    print(f"Processing ticker: {ticker}")
                    print(f"{'='*60}")
                    
                    # Fetch filings using sec-api.io
                    filings = fetch_sec_10k_filings(ticker, limit=limit_per_ticker)
                    
                    if not filings:
                        print(f"No filings found for {ticker}, skipping...")
                        continue
                    
                    try:
                        # Download and upload each filing
                        for i, filing in enumerate(filings, 1):
                            print(f"\n[{i}/{len(filings)}] Processing filing for {ticker}...")
                            
                            # Download the document
                            file_path = download_filing_document(filing, temp_path, download_pdf=download_pdf)
                            
                            if not file_path:
                                print(f"Failed to download, skipping...")
                                continue
                            
                            # Calculate file checksum
                            file_checksum = calculate_file_checksum(file_path)
                            
                            # Extract metadata
                            accession_no = filing.get('accessionNo', 'N/A')
                            cik = filing.get('cik', 'N/A')
                            filing_date = filing.get('filedAt', '')[:10] if filing.get('filedAt') else None
                            report_date = filing.get('periodOfReport', '')[:10] if filing.get('periodOfReport') else None
                            form_type = filing.get('formType', '10-K')
                            file_name = Path(file_path).name
                            document_url = filing.get('linkToFilingDetails', '')
                            
                            # Check if file with same checksum already exists
                            check_sql = f"""
                            SELECT COUNT(*) as cnt
                            FROM {table_name}
                            WHERE ticker = '{ticker}'
                            AND accession_number = '{accession_no}'
                            AND file_checksum = '{file_checksum}'
                            """
                            existing_count = session.sql(check_sql).collect()[0]['CNT']
                            
                            if existing_count > 0:
                                print(f"⊘ File unchanged, skipping upload: {file_name} (checksum: {file_checksum[:8]}...)")
                                continue
                            
                            # Upload file to Snowflake stage using PUT command
                            print(f"Uploading to Snowflake stage...")
                            put_sql = f"PUT file://{file_path} @{stage_name}/{ticker}/ AUTO_COMPRESS=FALSE"
                            session.sql(put_sql).collect()
                            
                            # Record metadata
                            all_filings.append({
                                'ticker': ticker,
                                'cik': cik,
                                'accession_number': accession_no,
                                'filing_date': filing_date,
                                'report_date': report_date,
                                'form_type': form_type,
                                'file_path': f'@{stage_name}/{ticker}/{file_name}',
                                'file_checksum': file_checksum,
                                'document_url': document_url,
                                'loaded_at': datetime.datetime.now()
                            })
                            
                            print(f"✓ Successfully uploaded {file_name} (checksum: {file_checksum[:8]}...)")

                    except Exception as e:
                        print("Failed filings for loop ", filing)
                        print(e)
            except Exception as e:
                print("Failed tickers for loop ", ticker)
                print(e)
        except Exception as e:
            print("Failed with clause")
            print(e)
    # Insert metadata into table
    if all_filings:
        print(f"\n{'='*60}")
        print(f"Inserting {len(all_filings)} filing records into metadata table...")
        df = session.create_dataframe(all_filings)
        df.write.mode("append").save_as_table(table_name)
        print(f"✓ Metadata inserted successfully")
    
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"✓ Successfully loaded {len(all_filings)} filings to stage: {stage_name}")
    print(f"✓ Metadata stored in table: {table_name}")
    
    # List files in stage
    print(f"\nFiles in stage:")
    list_sql = f"LIST @{stage_name}"
    files = session.sql(list_sql).collect()
    for file in files[:20]:  # Show first 20
        print(f"  - {file[0]}")
    
    if len(files) > 20:
        print(f"  ... and {len(files) - 20} more files")
    
    return {
        'stage': stage_name,
        'table': table_name,
        'filings_loaded': len(all_filings),
        'tickers': tickers
    }

load_sec_filings_to_snowflake(['AAPL', 'TSLA'], os.environ["SF_SCHEMA"], 20, True)