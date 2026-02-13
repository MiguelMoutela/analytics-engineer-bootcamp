CREATE OR REPLACE TABLE miguelmoutela.world_inequality_text_chunks (
    chunk_id VARCHAR,
    file_id VARCHAR,
    filename VARCHAR,
    page_number NUMBER,
    paragraph_number NUMBER,
    chunk_type VARCHAR,               
    text_content VARCHAR,
    embedding VECTOR(FLOAT, 768),     -- text embedding model size
    metadata VARIANT,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE miguelmoutela.world_inequality_images (
    image_id VARCHAR,
    image_number VARCHAR,
    file_id VARCHAR,
    image_base64 VARCHAR,
    image_url VARCHAR,
    embedding VECTOR(FLOAT, 1024),  
    metadata VARIANT,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


INSERT INTO dataexpert_student.miguelmoutela.world_inequality_text_chunks (
    chunk_id,
    file_id,
    filename,
    page_number,
    paragraph_number,
    chunk_type,
    text_content,
    embedding,
    metadata,
    processed_at
)
SELECT
    UUID_STRING() AS chunk_id,
    t.file_checksum AS file_id,
    t.filename,
    t.page_number,
    t.paragraph_number,
    'PARAGRAPH' AS chunk_type,
    t.paragraph AS text_content,
    AI_EMBED(
        'snowflake-arctic-embed-m',
        t.paragraph
    ) AS embedding,
    -- Structured metadata
    OBJECT_CONSTRUCT_KEEP_NULL(
        'value_hint', value_hint,
        'paragraph_length', LENGTH(t.paragraph)
    ) AS metadata,
    CURRENT_TIMESTAMP() as processed_at
FROM (
-- text extract
with page_paragraphs as (
    select
        file_checksum,
        filename,
        page_number + 1 as page_number,
        image_id,
        t.index as paragraph_number,
        trim(t.value) as paragraph
    from dataexpert_student.miguelmoutela.world_inequality_reports_layout_raw,
         lateral split_to_table(page_text, '\n\n') t(line)
    where contains(paragraph, image_id) or image_id is null
)
, ref_images as (
    select *, 
        array_agg(image_id) over (
            partition by file_checksum, filename, page_number, paragraph_number order by paragraph_number
        ) as images_in_paragraph
    from page_paragraphs
), hinted_paragraphs as (
    select 
        p.file_checksum,
        p.filename,
        p.page_number,
        r.image_id,
        p.paragraph_number,
        p.paragraph,
        object_construct(
            'type', case
                    when startswith(p.paragraph, '#')
                        or startswith(p.paragraph, 'Source:')
                        or startswith(p.paragraph, 'Notes:')
                        or startswith(p.paragraph, '[^0]')
                            then p.paragraph
                        else null
                    end,
            'images', case 
                      when array_size(r.images_in_paragraph) > 0
                          then images_in_paragraph
                          else null
                      end
        ) AS value_hint
    from page_paragraphs p
    left join ref_images r on (
        p.file_checksum = r.file_checksum 
        and p.filename = r.filename
        and p.page_number = r.page_number
        and p.paragraph_number = r.paragraph_number
        and p.image_id = r.image_id
    )
), nullable_hints as (
    select 
        file_checksum,
        filename,
        page_number,
        image_id,
        paragraph_number,
        paragraph,
        case
            when value_hint = object_construct()
                then null
            else value_hint
        end as value_hint
    from hinted_paragraphs
)
select *,
    coalesce(
        last_value(value_hint ignore nulls) over (
            partition by file_checksum
            order by page_number, paragraph_number
            rows between unbounded preceding and current row
        ),
        object_construct() 
    ) AS value_hint_filled
from nullable_hints
where length(paragraph) > 50
order by file_checksum, page_number, paragraph_number
) t
;


select * from miguelmoutela.world_inequality_text_chunks;
