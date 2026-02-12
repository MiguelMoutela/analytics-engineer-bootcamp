list @MIGUELMOUTELA.WORLD_INEQUALITY_REPORTS_STAGE;


create or replace table miguelmoutela.earnings_transcripts_metadata as
select *
from miguelmoutela.sec_filings_metadata
where 1=2;


select *
from miguelmoutela.document_processing_log;

SELECT 
    m.ticker,
    m.accession_number,
    m.file_path,
    m.file_checksum
FROM miguelmoutela.sec_filings_metadata m
WHERE m.file_checksum NOT IN (
    SELECT DISTINCT file_checksum 
    FROM miguelmoutela.document_processing_log
    WHERE status = 'SUCCESS'
    AND file_checksum 
)
ORDER BY m.ticker, m.filing_date DESC;






SELECT 
    m.ticker,
    m.accession_number,
    m.file_path,
    m.file_checksum
FROM miguelmoutela.sec_filings_metadata m
WHERE NOT EXISTS (
    SELECT 1
    FROM miguelmoutela.document_processing_log d
    WHERE d.status = 'SUCCESS'
      AND d.file_checksum = m.file_checksum
)
ORDER BY m.ticker, m.filing_date DESC;



select * from MIGUELMOUTELA.world_inequality_reports_metadata;

DROP STAGE MIGUELMOUTELA.WORLD_INEQUALITY_REPORTS_STAGE;






