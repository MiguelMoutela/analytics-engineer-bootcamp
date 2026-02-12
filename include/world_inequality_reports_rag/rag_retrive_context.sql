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
        t.chunk_id,
        t.file_id,
        t.text_content,
        t.metadata,
        vector_cosine_similarity(t.embedding, q.q_vec_768) as score
    from miguelmoutela.world_inequality_text_chunks t,
         query_embedding q
)
, chunks as (
    select 
        chk.chunk_id,
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
        chk.chunk_id,
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
