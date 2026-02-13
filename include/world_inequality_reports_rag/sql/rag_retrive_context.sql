with query_embedding as (
    select 
        ai_embed(
            'snowflake-arctic-embed-m',
            'What are the main drivers of carbon inequality in middle income countries?'
    ) as q_vec_768,
        ai_embed(
            'snowflake-arctic-embed-l-v2.0',
            'What are the main drivers of carbon inequality in middle income countries?'
    ) as q_vec_1024
)
, ranked_chunks as (
    select
        t.file_id,
        t.text_content,
        t.metadata,
        vector_cosine_similarity(t.embedding, q.q_vec_768) as score
    from miguelmoutela.world_inequality_text_chunks t,
         query_embedding q
)
, chunks as (
    select 
        chk.file_id,
        chk.text_content,
        img.value as image_number,
        chk.metadata as text_metadata,
        chk.score
    from ranked_chunks chk,
    lateral flatten(input => chk.metadata:value_hint.images, outer => true) img
    where score > 0.75
)
, chunks_with_images as (
    select 
        chk.file_id,
        chk.text_content,
        chk.text_metadata,
        chk.score,
        img.image_id,
        img.image_number,
        img.image_url,
        img.metadata as img_metadata
    from chunks chk
    left join miguelmoutela.world_inequality_images img on (
        chk.file_id = img.file_id and
        chk.image_number = img.image_number
    )
)
select *
from chunks_with_images
order by score desc;


--idea gemini --looks good



WITH query_embedding AS (
    SELECT SNOWFLAKE.CORTEX.AI_EMBED_768(
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
    FROM miguelmoutela.world_inequality_text_chunks t,
         query_embedding q
)
, chunks as (
    select *
    from ranked_chunks
    where score > 0.75
)
,chunks_with_images AS (
    SELECT 
        chk.text_content,
        -- Generate the TO_FILE object for every image linked to the text
        TO_FILE('@miguelmoutela.world_inequality_report_images_stage', img.image_id) AS img_file
    FROM chunks chk
    LEFT JOIN miguelmoutela.world_inequality_images img ON (
        chk.file_id = img.file_id AND 
        ARRAY_CONTAINS(img.image_number::VARIANT, chk.metadata:value_hint.images)
    )
),
final_context AS (
    SELECT 
        -- Combine all unique text chunks
        LISTAGG(DISTINCT text_content, '\n\n') AS full_text,
        -- Aggregate all image file objects into one array
        ARRAY_AGG(DISTINCT img_file) WITHIN GROUP (ORDER BY img_file) AS file_array
    FROM chunks_with_images
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'voyage-multimodal-3', -- Or 'claude-3-5-sonnet' if available
    OBJECT_CONSTRUCT(
        'messages', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'role','system',
                'content','You are a report analyst. Answer the user question using the provided text and images.'
            ),
            OBJECT_CONSTRUCT(
                'role','user',
                'content', CONCAT(
                    'Context Information:\n', 
                    (SELECT full_text FROM final_context),
                    '\n\nQuestion: What does the chart reveal about middle-income countries?'
                )
            )
        ),
        'files', (SELECT file_array FROM final_context)
    )
) AS ai_response;
