import os
import requests
import tempfile
import hashlib
from pathlib import Path
from urllib.parse import urlparse
from typing import List, Dict
from bs4 import BeautifulSoup
import dataexpert_snowflake as snowflake
import datetime


def calculate_file_checksum(file_path: str) -> str:
    """
    Calculate md5 checksum of a file. md5 matches Snowflake Stage checksum.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Hexadecimal checksum string
    """
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        # Read file in chunks to handle large files efficiently
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()



def calculate_file_checksum(file_path: str) -> str:
     # Switch from sha256 to md5
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()


def fetch_wir_reports() -> List[Dict]:
    """
    Fetch World Inequality Reports from //wid.world
    
    Returns:
        List of reports metadata dictionaries
    """
    try:        
        # Query for the library-key-reports data tag of report containers
        url = "https://wid.world/methodology/#library-key-reports"
        response = requests.get(url)

        soup = BeautifulSoup(response.text, "html.parser")
        key_reports_container = soup.find("div", id="library-key-reports-listcontainer")

        key_report_rows = key_reports_container.find_all("a", href=True)

        key_reports = [
            {
                "url": row["href"], 
                "authors": authors.strip() + ".", 
                "title": title.strip(),
                "safe_title": Path(urlparse(row["href"]).path).name
            }
            for row in key_report_rows
            for authors, title in [row.get_text(strip=True).rsplit('.', 1)]
        ]

        return key_reports

    except Exception as e:
        print(f"Error fetching key reports from //wid.world. Exception: {e}")
        return []


def download_transcript_document(report_details: Dict, temp_dir: Path) -> str:
    """
    Download a report document.
    
    Args:
        report_details: report metadata from //wid.world
        temp_dir: Temporary directory to save files
    
    Returns:
        Path to downloaded file
    """
    try:
        url = report_details.get('url')
        if not url:
            print(f"No text link available for {report_details.get('title')}")
            return None
        
        print(f"Downloading transcript for {report_details.get('title')} ...")
        response = requests.get(url)
        response.raise_for_status()
        
        file_path = temp_dir / f"{report_details.get('safe_title')}.pdf"
        file_path.write_bytes(response.content)
        print(f"Downloaded transcript: {file_path}")
        return str(file_path)
            
    except Exception as e:
        print(f"Error downloading transcript {report_details.get('title', 'unknown')}: {e}")
        return None


def load_earnings_transcripts_to_snowflake():
    """
    Load World Inequality Reports into a Snowflake stage using Snowpark.
    """
    # Get schema from environment if not provided
    schema = os.getenv("STUDENT_SCHEMA") or os.getenv("SCHEMA")
    
    # Get Snowpark session
    session = snowflake.get_snowpark_session(schema=schema)
    print(f"Connected to Snowflake with schema: {schema}")
    
    # Create stage for earnings call transcripts
    stage_name = f"{schema}.world_inequality_reports_stage"
    create_stage_sql = f"""
    CREATE STAGE IF NOT EXISTS {stage_name}
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for World Inequality Reports'
    """
    
    print(f"Creating stage: {stage_name}")
    session.sql(create_stage_sql).collect()
    
    # Create table to track transcripts metadata
    table_name = f"{schema}.world_inequality_reports_metadata"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        title VARCHAR,
        authors VARCHAR,
        file_name VARCHAR,
        file_path VARCHAR,
        file_checksum VARCHAR,
        document_url VARCHAR,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        raw_processed_at TIMESTAMP,
        images_extracted_at TIMESTAMP,
        images_embedded_at TIMESTAMP
    )
    """
        
    print(f"Creating metadata table: {table_name}")
    session.sql(create_table_sql).collect()
    
    # Create temporary directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        all_reports = []
        
        # Fetch and download reports
        for report in fetch_wir_reports():
            print(f"\n{'='*60}")
            print(f"Processing report: {report.get('title')}")
            print(f"{'='*60}")
            
            if not report.get('title'):
                print(f"No title found, skipping...")
                continue
                
            file_path = download_transcript_document(report, temp_path)
            if not file_path:
                print(f"Failed to download, skipping...")
                continue
                
            # Calculate file checksum
            file_checksum = calculate_file_checksum(file_path)
                
            # Extract metadata
            title = report.get('title')
            authors = report.get('authors')
            file_name = Path(file_path).name
            document_url = report.get('url')
 
            # Check if file with same checksum already exists
            check_sql = f"""
            SELECT COUNT(*) as cnt
            FROM {table_name}
            WHERE title = '{title}'
            AND authors = '{authors}'
            AND file_checksum = '{file_checksum}'
            """
            existing_count = session.sql(check_sql).collect()[0]['CNT']
                
            if existing_count > 0:
                print(f"⊘ File unchanged, skipping upload: {file_name} (checksum: {file_checksum[:8]}...)")
                continue
                
            # Upload file to Snowflake stage using PUT command
            print(f"Uploading to Snowflake stage...")
            put_sql = f'PUT file://{file_path} @{stage_name} AUTO_COMPRESS=FALSE'
            session.sql(put_sql).collect()
                
            # Record metadata
            all_reports.append({
                'title': title,
                'authors': authors,
                'file_name': file_name,
                'file_path': f'@{stage_name}/{file_name}',
                'file_checksum': file_checksum,
                'document_url': document_url,
                'loaded_at': datetime.datetime.now()
            })
            
            print(f"✓ Successfully uploaded {file_name} (checksum: {file_checksum[:8]}...)")
    
    # Insert metadata into table
    if all_reports:
        print(f"\n{'='*60}")
        print(f"Inserting {len(all_reports)} report-records into metadata table...")
        df = session.create_dataframe(all_reports)
        df.write.mode("append").save_as_table(table_name)
        print(f"✓ Metadata inserted successfully")
    
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"✓ Successfully loaded {len(all_reports)} reports to stage: {stage_name}")
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
        'reports_loaded': len(all_reports),
        'reports': all_reports
    }


load_earnings_transcripts_to_snowflake()