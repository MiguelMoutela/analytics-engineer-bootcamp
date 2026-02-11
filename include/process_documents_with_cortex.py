import os
from typing import List, Dict, Optional
import dataexpert_snowflake as snowflake
import datetime


def process_documents_with_cortex_ai(
    schema: str = None,
    embedding_model: str = "snowflake-arctic-embed-m-v1.5",
    tickers: List[str] = None
):
    """
    Process SEC filings and earnings transcripts using Snowflake Cortex AI.
    Extracts paragraphs page by page, creates embeddings, and maintains document structure.
    
    This function:
    1. Reads documents from Snowflake stages (PDFs and text files)
    2. Uses PARSE_DOCUMENT to extract structured content (paragraphs, tables, images)
    3. Processes content page by page and paragraph by paragraph
    4. Generates embeddings using Cortex EMBED_TEXT_1024
    5. Stores results with proper relationships between elements
    
    Args:
        schema: Snowflake schema to use (defaults to SCHEMA env var)
        embedding_model: Cortex embedding model to use
        tickers: Optional list of tickers to process (defaults to all)
    
    Returns:
        Dictionary with processing results
    """
    # Get schema from environment if not provided
    if schema is None:
        schema = os.getenv("STUDENT_SCHEMA") or os.getenv("SCHEMA")
    
    # Get Snowpark session
    session = snowflake.get_snowpark_session(schema=schema)
    print(f"Connected to Snowflake with schema: {schema}")
    
    # Create table for document chunks with embeddings
    chunks_table = f"{schema}.document_chunks_with_embeddings"
    create_chunks_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {chunks_table} (
        chunk_id VARCHAR,
        source_type VARCHAR,  -- 'SEC_FILING' or 'TRANSCRIPT'
        ticker VARCHAR,
        accession_number VARCHAR,
        file_path VARCHAR,
        page_number INTEGER,
        paragraph_number INTEGER,
        chunk_type VARCHAR,  -- 'PARAGRAPH', 'TABLE', 'IMAGE'
        content VARIANT,  -- Full extracted content as JSON
        text_content VARCHAR,  -- Plain text for embeddings
        embedding VECTOR(FLOAT, 1024),  -- Embedding vector
        parent_chunk_id VARCHAR,  -- For maintaining relationships
        metadata VARIANT,  -- Additional metadata (table structure, image refs, etc.)
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """
    
    print(f"Creating chunks table: {chunks_table}")
    session.sql(create_chunks_table_sql).collect()
    
    # Create table for tracking processed documents
    processing_log_table = f"{schema}.document_processing_log"
    create_log_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {processing_log_table} (
        file_path VARCHAR,
        file_checksum VARCHAR,
        source_type VARCHAR,
        ticker VARCHAR,
        status VARCHAR,  -- 'SUCCESS', 'FAILED', 'PROCESSING'
        chunks_created INTEGER,
        error_message VARCHAR,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (file_checksum, processed_at)
    )
    """
    
    print(f"Creating processing log table: {processing_log_table}")
    session.sql(create_log_table_sql).collect()
    
    # Process SEC filings (PDFs)
    print(f"\n{'='*60}")
    print(f"Processing SEC Filings with Cortex AI")
    print(f"{'='*60}")

    sec_filings_processed = process_sec_filings(
        session, schema, chunks_table, processing_log_table,
        embedding_model, tickers
    )

    # Process earnings transcripts (text files)
    print(f"\n{'='*60}")
    print(f"Processing Earnings Transcripts with Cortex AI")
    print(f"{'='*60}")
    
    transcripts_processed = process_transcripts(
        session, schema, chunks_table, processing_log_table,
        embedding_model, tickers
    )
    
    # Summary
    print(f"\n{'='*60}")
    print(f"CORTEX AI PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"✓ SEC Filings processed: {sec_filings_processed}")
    print(f"✓ Transcripts processed: {transcripts_processed}")
    print(f"✓ Chunks stored in: {chunks_table}")
    print(f"✓ Processing log: {processing_log_table}")
    
    return {
        'chunks_table': chunks_table,
        'processing_log': processing_log_table,
        'sec_filings_processed': sec_filings_processed,
        'transcripts_processed': transcripts_processed
    }


def process_sec_filings(
    session,
    schema: str,
    chunks_table: str,
    log_table: str,
    embedding_model: str,
    tickers: Optional[List[str]] = None
):
    """Process SEC filing PDFs using Cortex PARSE_DOCUMENT."""
    
    # Get list of files to process
    metadata_table = f"{schema}.sec_filings_metadata"
    
    if tickers:
        ticker_filter = "AND ticker IN ('" + "', '".join(tickers) + "')"
    else:
        ticker_filter = ""
    query = f"""
    SELECT 
        m.ticker,
        m.accession_number,
        m.file_path,
        m.file_checksum,
        m.form_type
    FROM {metadata_table} m
    WHERE m.file_checksum NOT IN (
        SELECT DISTINCT file_checksum 
        FROM {log_table} 
        WHERE status = 'SUCCESS'
    )
    {ticker_filter}
    ORDER BY m.ticker, m.filing_date DESC
    """
    
    print(f"Fetching SEC filings to process...")
    filings_df = session.sql(query).collect()
    print(f"Found {len(filings_df)} SEC filings to process")
    processed_count = 0
    for filing in filings_df:
        ticker = filing['TICKER']
        accession_number = filing['ACCESSION_NUMBER']
        file_path = filing['FILE_PATH']
        file_checksum = filing['FILE_CHECKSUM']
        
        print(f"\nProcessing: {ticker} - {accession_number} (checksum: {file_checksum[:8]}...)")
        
        try:
            # Split file_path into stage and relative path for BUILD_SCOPED_FILE_URL
            # Format: @stage_name/relative/path
            stage_end_idx = file_path.find('/', 1)  # Find first / after @
            stage = file_path[:stage_end_idx] if stage_end_idx > 0 else file_path
            relative_path = file_path[stage_end_idx+1:] if stage_end_idx > 0 else ''
            # Use Cortex PARSE_DOCUMENT to extract structured content from PDF
            # This extracts text, tables, images with page-level structure
            parse_sql = f"""
            WITH parsed_doc AS (
                SELECT 
                    AI_PARSE_DOCUMENT(
                        TO_FILE('{stage}', '{relative_path}'),
                        OBJECT_CONSTRUCT('mode','layout', 'page_split', true, 'extract_images', true)
                    ) as parsed_content
            ),
            -- Extract pages with their content
            page_content AS (
                SELECT 
                    page.value:page_number::INTEGER as page_number,
                    page.value:content::VARCHAR as page_text
                FROM parsed_doc,
                    LATERAL FLATTEN(input => parsed_content:pages) page
                WHERE page.value:content IS NOT NULL
            ),
            -- Split each page's content into paragraphs by double newlines
            page_paragraphs AS (
                SELECT 
                    page_number,
                    SPLIT(page_text, '\\n\\n') as paragraph_array
                FROM page_content
            ),
            -- Flatten and number paragraphs within each page
            numbered_paragraphs AS (
                SELECT 
                    page_number,
                    para.index + 1 as paragraph_number,
                    TRIM(para.value::VARCHAR) as text_content
                FROM page_paragraphs,
                    LATERAL FLATTEN(input => paragraph_array) para
                WHERE LENGTH(TRIM(para.value::VARCHAR)) > 50  -- Filter out short lines
            )
            -- Create chunks with embeddings
            SELECT 
                UUID_STRING() as chunk_id,
                'SEC_FILING' as source_type,
                '{ticker}' as ticker,
                '{accession_number}' as accession_number,
                '{file_path}' as file_path,
                page_number,
                paragraph_number,
                'PARAGRAPH' as chunk_type,
                OBJECT_CONSTRUCT('text', text_content) as content,
                text_content,
                NULL as embedding,
                NULL as parent_chunk_id,
                OBJECT_CONSTRUCT('paragraph_length', LENGTH(text_content)) as metadata,
                CURRENT_TIMESTAMP() as processed_at
            FROM numbered_paragraphs
            """

            print(parse_sql)
            
            # Insert chunks into table
            insert_sql = f"""
            INSERT INTO {chunks_table} (
                chunk_id, source_type, ticker, accession_number, file_path,
                page_number, paragraph_number, chunk_type, content, text_content,
                embedding, parent_chunk_id, metadata, processed_at
            )
            {parse_sql}
            """
            
            result = session.sql(insert_sql).collect()
            chunks_created = result[0][0] if result else 0
            
            # Log success
            log_sql = f"""
            INSERT INTO {log_table} (file_path, file_checksum, source_type, ticker, status, chunks_created, processed_at)
            VALUES ('{file_path}', '{file_checksum}', 'SEC_FILING', '{ticker}', 'SUCCESS', {chunks_created}, CURRENT_TIMESTAMP())
            """
            session.sql(log_sql).collect()
            
            print(f"✓ Created {chunks_created} chunks from {ticker} filing")
            processed_count += 1
            
        except Exception as e:
            print(f"✗ Error processing {ticker} - {accession_number}: {e}")
            
            # Log failure
            error_msg = str(e).replace("'", "''")
            log_sql = f"""
            INSERT INTO {log_table} (file_path, file_checksum, source_type, ticker, status, error_message, processed_at)
            VALUES ('{file_path}', '{file_checksum}', 'SEC_FILING', '{ticker}', 'FAILED', '{error_msg}', CURRENT_TIMESTAMP())
            """
            session.sql(log_sql).collect()
    
    return processed_count


def process_transcripts(
    session,
    schema: str,
    chunks_table: str,
    log_table: str,
    embedding_model: str,
    tickers: Optional[List[str]] = None
):
    """Process earnings transcript text files using Cortex."""
    
    # Get list of files to process
    metadata_table = f"{schema}.earnings_transcripts_metadata"
    
    if tickers:
        ticker_filter = "AND ticker IN ('" + "', '".join(tickers) + "')"
    else:
        ticker_filter = ""
    
    query = f"""
    SELECT 
        m.ticker,
        m.accession_number,
        m.file_path,
        m.file_checksum
    FROM {metadata_table} m
    WHERE m.file_checksum NOT IN (
        SELECT DISTINCT file_checksum 
        FROM {log_table} 
        WHERE status = 'SUCCESS'
    )
    {ticker_filter}
    ORDER BY m.ticker, m.filing_date DESC
    """
    
    print(f"Fetching transcripts to process...")
    transcripts_df = session.sql(query).collect()
    print(f"Found {len(transcripts_df)} transcripts to process")
    
    processed_count = 0
    for transcript in transcripts_df:
        ticker = transcript['TICKER']
        accession_number = transcript['ACCESSION_NUMBER']
        file_path = transcript['FILE_PATH']
        file_checksum = transcript['FILE_CHECKSUM']
        
        print(f"\nProcessing: {ticker} - {accession_number} (checksum: {file_checksum[:8]}...)")
        print(f"\nProcessing: {ticker} - {accession_number}")
        
        try:
            # Split file_path into stage and relative path for BUILD_SCOPED_FILE_URL
            # Format: @stage_name/relative/path
            stage_end_idx = file_path.find('/', 1)  # Find first / after @
            stage = file_path[:stage_end_idx] if stage_end_idx > 0 else file_path
            relative_path = file_path[stage_end_idx+1:] if stage_end_idx > 0 else ''
            # Read and parse text file into paragraphs
            # Split by double newlines (common paragraph delimiter)
            parse_sql = f"""
            WITH file_content AS (
                SELECT 
                        AI_PARSE_DOCUMENT(
                            TO_FILE('{stage}', '{relative_path}'),
                          OBJECT_CONSTRUCT('mode','OCR')
                        ):content as full_text
            ),
            -- Split into paragraphs (by double newline or section headers)
            paragraphs AS (
                SELECT 
                    SPLIT(full_text, '<span>') as paragraph_array
                FROM file_content
            ),
            -- Flatten and number paragraphs
            numbered_paragraphs AS (
                SELECT 
                    para.index + 1 as paragraph_number,
                    TRIM(para.value::VARCHAR) as text_content
                FROM paragraphs,
                    LATERAL FLATTEN(input => paragraph_array) para
                WHERE LENGTH(TRIM(para.value::VARCHAR)) > 50  -- Filter out short lines
            )
            -- Create chunks with embeddings
            SELECT 
                UUID_STRING() as chunk_id,
                'TRANSCRIPT' as source_type,
                '{ticker}' as ticker,
                '{accession_number}' as accession_number,
                '{file_path}' as file_path,
                FLOOR((paragraph_number - 1) / 10) + 1 as page_number,  -- Estimate pages (10 paragraphs per page)
                paragraph_number,
                'PARAGRAPH' as chunk_type,
                OBJECT_CONSTRUCT('text', text_content) as content,
                text_content,
                NULL as embedding,
                NULL as parent_chunk_id,
                OBJECT_CONSTRUCT('paragraph_length', LENGTH(text_content)) as metadata,
                CURRENT_TIMESTAMP() as processed_at
            FROM numbered_paragraphs
            """

            
            # Insert chunks into table
            insert_sql = f"""
            INSERT INTO {chunks_table} (
                chunk_id, source_type, ticker, accession_number, file_path,
                page_number, paragraph_number, chunk_type, content, text_content,
                embedding, parent_chunk_id, metadata, processed_at
            )
            {parse_sql}
            """
            
            result = session.sql(insert_sql).collect()
            chunks_created = result[0][0] if result else 0
            
            # Log success
            log_sql = f"""
            INSERT INTO {log_table} (file_path, file_checksum, source_type, ticker, status, chunks_created, processed_at)
            VALUES ('{file_path}', '{file_checksum}', 'TRANSCRIPT', '{ticker}', 'SUCCESS', {chunks_created}, CURRENT_TIMESTAMP())
            """
            session.sql(log_sql).collect()
            
            print(f"✓ Created {chunks_created} chunks from {ticker} transcript (checksum: {file_checksum[:8]}...)")
            processed_count += 1
            
        except Exception as e:
            print(f"✗ Error processing {ticker} - {accession_number}: {e}")
            
            # Log failure
            error_msg = str(e).replace("'", "''")
            log_sql = f"""
            INSERT INTO {log_table} (file_path, file_checksum, source_type, ticker, status, error_message, processed_at)
            VALUES ('{file_path}', '{file_checksum}', 'TRANSCRIPT', '{ticker}', 'FAILED', '{error_msg}', CURRENT_TIMESTAMP())
            """
            session.sql(log_sql).collect()
    
    return processed_count

process_documents_with_cortex_ai('MIGUELMOUTELA', embedding_model='snowflake-arctic-embed-m-v1.5')