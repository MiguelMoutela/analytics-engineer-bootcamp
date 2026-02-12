list @miguelmoutela.sec_filings_stage;
create or replace table miguelmoutela.extracted_transcripts as
select *
from (
    with filings as (
        select 
            split_part("name", '/', 1) as stage_name,
            split_part("name", '/', 2) as ticker,
            split_part("name", '/', 3) as file_suffix,
        from table(result_scan(last_query_id()))
    )
    select 
        ticker, 
        concat(ticker, '/', file_suffix) as file_name,
        split_part(file_suffix, '_', 2) as report_date,
        snowflake.cortex.ai_extract(
            file => to_file('@miguelmoutela.sec_filings_stage', concat(ticker, '/', file_suffix)),
            responseFormat => {
                'schema': {
                    'type': 'object', 
                    'properties': {
                        'competitors': {
                            'type': 'array', 
                            'items': {
                                'type': 'string',
                                'properties': {
                                    'name': { 'type': 'string', 'description': 'competitor name mentioned; ensure these are actual company names not another classification like market-segments. ensure the company names mentioned are not part of the parent group' }
                                }
                            }
                        },
                        'revenue': { 
                            'type': 'string',
                            'description': 'Total reported revenue in USD, Convert billions or millions to absolute dollars; null if n/a'
                        },
                        'units_sold': { 
                            'type': 'string',
                            'description': 'Total units sold or delivered in the period. Conver phrases like "1.8 million vehicles to 1800000"; null if n/a'
                        },
                        'risks': { 
                            'type': 'array',
                            'items': {
                                'type': 'string',
                                'description': ''
                            }
                        }
                    }
                }
            }
        ) as extracted
    from filings f
);


create or replace table miguelmoutela.earnings_transcript_extracts (
    ticker varchar(4),
    report_date date,
    file_name varchar(128) not null,
    extracted_value object(
        competitors array(varchar(64)),
        revenue integer,
        units_sold integer,
        risks array(varchar)
    )
);


insert into miguelmoutela.earnings_transcript_extracts
select 
    ticker,
    report_date,
    file_name,
    object_construct(
        'competitors', coalesce(extracted:response:competitors::array, array_construct()),
        'revenue', coalesce(try_cast(extracted:response:revenue::string as integer), -1),
        'units_sold', coalesce(try_cast(extracted:response:units_sold::string as integer), -1),
        'risks', coalesce(extracted:response:risks::array, array_construct())
    )::object (
        competitors array(varchar(64)),
        revenue integer,
        units_sold integer,
        risks array(varchar)
    ) as extracted_value
from miguelmoutela.extracted_transcripts
;




--other

select * from miguelmoutela.document_chunks_with_embeddings;
select * from miguelmoutela.document_processing_log;
select * from miguelmoutela.embeddings_to_search;

WITH parsed_doc AS (
                SELECT 
                    AI_PARSE_DOCUMENT(
                        TO_FILE('@MIGUELMOUTELA.sec_filings_stage', 'AAPL/AAPL_2018-11-05_000032019318000145.pdf'),
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
                    SPLIT(page_text, '\n\n') as paragraph_array
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
                'AAPL' as ticker,
                '0000320193-18-000145' as accession_number,
                '@MIGUELMOUTELA.sec_filings_stage/AAPL/AAPL_2018-11-05_000032019318000145.pdf' as file_path,
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