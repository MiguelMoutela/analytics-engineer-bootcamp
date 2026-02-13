import os

def list_stage(stage_name: str):
    return fr"""--sql
        list @{os.environ["SF_SCHEMA"]}.{stage_name};
    """

def update_metadata_as_at(as_at_column: str):
    return fr"""--sql
        UPDATE {os.environ["SF_SCHEMA"]}.world_inequality_reports_metadata
        SET {as_at_column} = (
            SELECT MAX(processed_at) 
            FROM {os.environ["SF_SCHEMA"]}.world_inequality_reports_layout_raw
        );
    """

def create_stage(stage_name: str): 
    return f"""--sql
    CREATE STAGE IF NOT EXISTS {os.environ["SF_SCHEMA"]}.{stage_name}
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
"""

CREATE_METADATA_TABLE = f"""--sql
    CREATE TABLE IF NOT EXISTS {os.environ["SF_SCHEMA"]}.WORLD_INEQUALITY_REPORTS_LAYOUT_RAW (
        FILE_CHECKSUM VARCHAR(16777216),
        FILENAME VARCHAR(16777216),
        PAGE_NUMBER NUMBER(38,0),
        PAGE_TEXT VARCHAR(16777216),
        IMAGE_ID VARCHAR(16777216),
        IMAGE_BASE64 VARCHAR(16777216),
        PROCESSED_AT TIMESTAMP
    );
"""

MERGE_RAW_TRANSFORM = fr"""--sql
    MERGE INTO {os.environ["SF_SCHEMA"]}.world_inequality_reports_layout_raw AS target
    USING (
        WITH report AS (
            SELECT 
                file_name AS filename,
                file_checksum
            FROM {os.environ["SF_SCHEMA"]}.world_inequality_reports_metadata
            WHERE raw_processed_at IS NULL
        ),
        parsed_doc AS (
            SELECT
                file_checksum,
                filename,
                SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
                    TO_FILE('@{os.environ["SF_SCHEMA"]}.world_inequality_reports_stage', filename),
                    {{ 'mode': 'LAYOUT', 'page_split': true, 'extract_images': true }}
                ) AS parsed_content
            FROM report
        )
        SELECT
            file_checksum,
            filename,
            page.value:index::INTEGER AS page_number,
            page.value:content::VARCHAR AS page_text,
            img.value:id::VARCHAR AS image_id,
            img.value:image_base64::VARCHAR AS image_base64,
            CURRENT_TIMESTAMP() AS processed_at
        FROM parsed_doc,
            LATERAL FLATTEN(INPUT => parsed_content:pages) page,
            LATERAL FLATTEN(INPUT => page.value:images, OUTER => TRUE) img
        WHERE page.value:content IS NOT NULL
    ) AS source
    ON  target.file_checksum = source.file_checksum 
    AND target.page_number = source.page_number
    AND (target.image_id = source.image_id OR (target.image_id IS NULL AND source.image_id IS NULL))

    WHEN MATCHED THEN 
        UPDATE SET processed_at = source.processed_at

    WHEN NOT MATCHED THEN
        INSERT (
            file_checksum, filename, page_number, page_text, image_id, image_base64, processed_at
        )
        VALUES (
            source.file_checksum, source.filename, source.page_number, source.page_text, source.image_id, source.image_base64, source.processed_at
        );
"""

MERGE_CHUNKS_TRANSFORM = fr"""--sql
merge into {os.environ["SF_SCHEMA"]}.world_inequality_text_chunks as target
using (
    with page_paragraphs as (
        select
            file_checksum,
            filename,
            page_number + 1 as page_number,
            image_id,
            t.index as paragraph_number,
            trim(t.value) as paragraph
        from {os.environ["SF_SCHEMA"]}.world_inequality_reports_layout_raw,
             lateral split_to_table(page_text, '\n\n') t(line)
        where contains(paragraph, image_id) or image_id is null
    )
    , ref_images as (
        select *, 
            array_agg(image_id) over (
                partition by file_checksum, filename, page_number, paragraph_number 
                order by paragraph_number
            ) as images_in_paragraph
        from page_paragraphs
    )
    , hinted_paragraphs as (
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
                            or startswith(p.paragraph, 'Interpretation:')
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
    select
        file_checksum as file_id,
        filename,
        page_number,
        paragraph_number,
        paragraph as text_content,
        coalesce(
            last_value(value_hint ignore nulls) over (
                partition by file_checksum
                order by page_number, paragraph_number
                rows between unbounded preceding and current row
            ),
            object_construct() 
        ) AS value_hint_filled
    from nullable_hints,
    where length(paragraph) > 50
    qualify row_number() over (
        partition by file_checksum, page_number, paragraph_number 
        order by image_id desc nulls last
    ) = 1
) as source
on  target.file_id = source.file_id
and target.page_number = source.page_number
and target.paragraph_number = source.paragraph_number

when matched then
    update set processed_at = current_timestamp()

when not matched then
    insert (
        file_id, filename, page_number, paragraph_number, 
        chunk_type, text_content, embedding, metadata, processed_at
    )
    values (
        source.file_id,
        source.filename,
        source.page_number,
        source.paragraph_number,
        'PARAGRAPH',
        source.text_content,
        AI_EMBED('snowflake-arctic-embed-m', source.text_content),
        OBJECT_CONSTRUCT_KEEP_NULL(
            'value_hint', source.value_hint_filled,
            'paragraph_length', length(source.text_content)
        ),
        current_timestamp()
    );
"""

def merge_images_transform(staged_files):
    a = fr"""--sql
    merge into {os.environ["SF_SCHEMA"]}.world_inequality_images as target
    using (
        with wir_images as (
            select * 
            from values({staged_files}) as t(image_url, image_filename, file_id, image_number)
        ),
        image_embeddings AS (
            select *,
                AI_EMBED(
                    'voyage-multimodal-3',
                    to_file('@{os.environ["SCHEMA"]}.world_inequality_report_images_stage', image_filename)
                ) as embedding
            from wir_images
        )
        select 
            img.image_url,
            img.filename,
            img.image_number as src_image_number,
            img.file_id as src_file_id,
            raw.image_base64,
            img.embedding,
            raw.page_number,
            raw.filename,
            object_construct(
                'page_number', page_number,
                'document', filename
            ) as metadata,
        from image_embeddings img
        join {os.environ["SF_SCHEMA"]}.world_inequality_reports_layout_raw raw on (
            raw.file_checksum = img.file_id
            and raw.image_id = img.image_id
        )
    ) as source
    on target.file_id = source.src_file_id 
    and target.image_number = source.src_image_number
    
    when matched then 
        update set 
            image_base64 = source.image_base64,
            image_url = source.image_url,
            metadata = source.metadata,
            processed_at = current_timestamp()
            
    when not matched then 
        insert (image_id, image_number, file_id, image_base64, image_url, embedding, metadata, processed_at)
        values (source.src_image_number, source.src_image_number, source.src_file_id, source.image_base64, source.image_url, source.embedding, source.metadata, current_timestamp())
    ;
    """
    return a
    
PROC_STAGE_BASE64_TO_JPEG_WIR_IMAGES = fr"""--sql
CREATE PROCEDURE IF NOT EXISTS {os.environ["SF_SCHEMA"]}.PROC_STAGE_BASE64_TO_JPEG_WIR_IMAGES()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS $$
import base64
import hashlib
import os

def run(session):
    stage_path = "@world_inequality_report_images_stage" # Ensure this stage exists
    
    # 1. Fetch data from your table and existing images in stage
    # The md5 column in LIST results is the hex hash
    
    staged_files = session.sql(f"LIST {{stage_path}}").collect()
    staged_names = {{os.path.basename(row["name"]) for row in staged_files}}
    
    df = session.table("{os.environ["SF_SCHEMA"]}.WORLD_INEQUALITY_REPORTS_LAYOUT_RAW").select("FILE_CHECKSUM", "IMAGE_ID", "IMAGE_BASE64").to_pandas()
    
    images_processed = 0
    images_staged = 0

    for index, row in df.iterrows():
        file_id = row["FILE_CHECKSUM"]
        img_id = row["IMAGE_ID"]
        b64_str = row["IMAGE_BASE64"]
        
        if not b64_str:
            continue
            
        # 1. Decode Base64, check the image has not been staged and write to local /tmp
        file_name = f"{{file_id}}_{{img_id}}"
        local_path = f"/tmp/{{file_name}}"
        
        # "guard for header eg data:image/jpeg;base64,/9j/4AAQSkZ..."
        if "," in b64_str:
            b64_str = b64_str.split(",")[1]

        # "idempontency check"
        if file_name in staged_names:
            continue

        # 2. Write the file and CLOSE it
        with open(local_path, "wb") as f:
            image_binary = base64.b64decode(b64_str)
            f.write(image_binary)

        images_staged += 1
            
        # 3. NOW upload the closed file and Clean up /tmp to prevent stale data
        session.file.put(local_path, stage_path, overwrite=True, auto_compress=False)
        os.remove(local_path) 
     
        images_processed += 1 

    return f"Successfully exported {{images_staged}} images to {{stage_path}}. Total images processed {{images_processed}}"
$$;
"""