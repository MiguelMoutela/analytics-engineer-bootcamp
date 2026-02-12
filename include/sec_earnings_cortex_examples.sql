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

