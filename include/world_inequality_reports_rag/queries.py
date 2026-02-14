import os

def list_stage(stage_name: str):
    return fr"""--sql
        list @{os.environ["SF_SCHEMA"]}.{stage_name};
    """
    
def _update_metadata_as_at(as_at_column: str, extraction_table: str):
    return fr"""--sql
        update {os.environ["SF_SCHEMA"]}.world_inequality_reports_metadata
        set as_at_column = (
            select max(processed_at) 
            from {os.environ["SF_SCHEMA"]}.extraction_table
        );
    """

def update_metadata_on_raw_extract_complete():
    return _update_metadata_as_at("raw_processed_at", "world_inequality_reports_layout_raw")

def update_metadata_on_text_proc_complete():
    return _update_metadata_as_at("text_processed_at", "world_inequality_reports_layout_raw")

def update_metadata_on_image_extract_complete():
    return _update_metadata_as_at("images_extracted_at", "world_inequality_reports_layout_raw")

def update_metadata_on_image_proc_complete():
    return _update_metadata_as_at("images_processed_at", "world_inequality_reports_layout_raw")

def last_updated_ts(table_name: str):
    return f"""--sql
        select max(processed_at) 
        from {os.environ["SF_SCHEMA"]}.{table_name};
    """

def create_stage(stage_name: str): 
    return f"""--sql
        CREATE STAGE IF NOT EXISTS {os.environ["SF_SCHEMA"]}.{stage_name}
        DIRECTORY = (ENABLE = TRUE)
        ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
        COMMENT = ?;
    """

CREATE_METADATA_TABLE = f"""--sql
    create table if not exists {os.environ["SF_SCHEMA"]}.world_inequality_reports_metadata (
        title varchar,
        authors varchar,
        file_name varchar,
        file_path varchar,
        file_checksum varchar,
        document_url varchar,
        loaded_at timestamp_ntz,
        raw_processed_at timestamp_ntz,
        text_processed_at timestamp_ntz,
        images_extracted_at timestamp_ntz,
        images_processed_at timestamp_ntz
    );
"""

CREATE_RAW_EXTRACT_TABLE = f"""--sql
    create table if not exists {os.environ["SF_SCHEMA"]}.world_inequality_reports_layout_raw (
        file_checksum varchar,
        filename varchar,
        page_number number(38,0),
        page_text varchar,
        image_id varchar,
        image_base64 varchar,
        processed_at varchar
    );
"""

CREATE_CHUNKS_TABLE = f"""--sql
    create table if not exists {os.environ["SF_SCHEMA"]}.world_inequality_reports_text_chunks (
        file_id varchar,
        filename varchar,
        page_number number,
        paragraph_number number,
        chunk_type varchar,               
        text_content varchar,
        embedding vector(float, 768),     
        metadata variant,
        processed_at timestamp_ntz
    );
"""

CREATE_IMAGES_TABLE = f"""--sql
    create table if not exists {os.environ["SF_SCHEMA"]}.world_inequality_reports_images (
        image_id varchar,
        image_number varchar,
        file_id varchar,
        image_base64 varchar,
        image_url varchar,
        embedding vector(float, 1024),  
        metadata variant,
        processed_at timestamp_ntz
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
        FROM parsed_doc,
            LATERAL FLATTEN(INPUT => parsed_content:pages) page,
            LATERAL FLATTEN(INPUT => page.value:images, OUTER => TRUE) img
        WHERE page.value:content IS NOT NULL
    ) AS source
    ON  target.file_checksum = source.file_checksum 
    AND target.page_number = source.page_number
    AND (target.image_id = source.image_id OR (target.image_id IS NULL AND source.image_id IS NULL))

    WHEN MATCHED THEN 
        UPDATE SET processed_at = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN
        INSERT (
            file_checksum, filename, page_number, page_text, image_id, image_base64, processed_at
        )
        VALUES (
            source.file_checksum, source.filename, source.page_number, source.page_text, source.image_id, source.image_base64, CURRENT_TIMESTAMP()
        );
"""

MERGE_CHUNKS_TRANSFORM = fr"""--sql
merge into {os.environ["SF_SCHEMA"]}.world_inequality_reports_text_chunks as target
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
    return fr"""--sql
    merge into {os.environ["SF_SCHEMA"]}.world_inequality_reports_images as target
    using (
        with wir_images as (
            select * 
            from values {staged_files} as t(image_url, stage_name, image_filename, file_id, image_number)
        ),
        image_embeddings as (
            select *,
                AI_EMBED(
                    'voyage-multimodal-3',
                    to_file(stage_name, image_filename)
                ) as embedding
            from wir_images
        )
        select 
            img.image_url,
            img.image_filename,
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
            and raw.image_id = img.image_number
        )
    ) as source
    on target.file_id = source.src_file_id 
    and target.image_number = source.src_image_number
    
    when matched then 
        update set 
            processed_at = current_timestamp()
            
    when not matched then 
        insert (image_id, image_number, file_id, image_base64, image_url, embedding, metadata, processed_at)
        values (source.image_filename, source.src_image_number, source.src_file_id, source.image_base64, source.image_url, source.embedding, source.metadata, current_timestamp())
    ;
    """
    
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
    stage_path = f"@{os.environ["SF_SCHEMA"]}world_inequality_reports_images_stage" # Ensure this stage exists
    
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

RAG_RETRIEVE_CONTEXT = f"""--sql
    WITH query_embedding AS (
        SELECT AI_EMBED(
            'snowflake-arctic-embed-m',
            'What are the main drivers of carbon inequality in middle income countries?'
        ) AS q_vec_768
    ),
    ranked_chunks AS (
        SELECT
            t.file_id,
            t.text_content,
            t.metadata,
            VECTOR_COSINE_SIMILARITY(t.embedding, q.q_vec_768) AS score
        FROM miguelmoutela.world_inequality_reports_text_chunks t,
             query_embedding q
        ORDER BY score DESC
        LIMIT 3
    ),
    chunks_with_images AS (
        SELECT 
            chk.text_content,
            TO_FILE(
                '@miguelmoutela.world_inequality_reports_images_stage',
                img.image_id
            ) AS img_file
        FROM ranked_chunks chk
        LEFT JOIN miguelmoutela.world_inequality_reports_images img
            ON chk.file_id = img.file_id
            AND ARRAY_CONTAINS(
                img.image_number::VARIANT,
                chk.metadata:value_hint.images
            )
    )
    SELECT 
        LISTAGG(DISTINCT text_content, '\n\n') AS full_text,
        ARRAY_AGG(DISTINCT img_file) AS file_array
    FROM chunks_with_images;
"""


def ai_complete(full_text, file_array_json):
    return f"""--sql
        SELECT AI_COMPLETE(
            'claude-3-5-sonnet',
            PROMPT(
                'Context: {{0}}\\n\\nQuestion: What are the main drivers of carbon inequality in middle income countries? Refer to these images: {{1}}',
                $${full_text}$$,
                PARSE_JSON($${file_array_json}$$)
            )
        );
    """
    