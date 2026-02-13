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








MERGE_IMAGES_TRANSFORM = fr"""--sql
still need to compile the proc to base64 to jpeg

INSERT INTO dataexpert_student.miguelmoutela.world_inequality_images (
    image_id,
    image_number,
    file_id,
    image_base64,
    image_url,
    embedding,
    metadata,
    created_at
)
select
    UUID_STRING() AS image_id,
    image_id as image_number,
    file_id,
    image_base64,
    image_url,
    embedding,
    object_construct(
        'page_number', page_number,
        'document', filename
    ) as metadata,
    current_timestamp()
from (
    with wir_images as (
        select 
            "name" as image_url,
            split_part("name", '/', -1) as image_filename
        from table(result_scan(last_query_id()))
    )   
    , image_embeddings as (
        select
            image_url,
            image_filename,
            split_part(image_filename, '_', -1) as image_id,
            split_part(image_filename, '_', -2) as file_id,
            AI_EMBED(
                'voyage-multimodal-3',
                to_file('@miguelmoutela.world_inequality_report_images_stage', image_filename)
            ) as embedding
        from wir_images
    )
    select 
        img.image_url,
        img.image_id,
        img.file_id,
        raw.image_base64,
        img.embedding,
        raw.page_number,
        raw.filename
    from image_embeddings img
    join miguelmoutela.world_inequality_reports_layout_raw raw on (
        raw.file_checksum = img.file_id
        and raw.image_id = img.image_id
    ) 
);
"""

304): 01c260dc-0107-c49c-0000-33eb03b66b8e: 100090 (42P18): Duplicate row detected during DML action
Row Values: ["536feb281fc101e9fe41dee116db875f3e2829b7b7ddc32e5db143d0a6bb682e", "world-inequality-report-2022.pdf", 58, 1, "PARAGRAPH", "Figure 2.3 Global income inequality: Gini index, 1820-2020
![img-46.jpeg](img-46.jpeg)", 