list @dataexpert_student.miguelmoutela.world_inequality_report_images_stage;
INSERT INTO dataexpert_student.miguelmoutela.world_inequality_images (
    image_id,
    file_id,
    filename,
    page_number,
    image_base64,
    image_url,
    embedding,
    metadata,
    created_at
)
select
    image_id,
    file_id,
    filename,
    page_number,
    image_base64,
    image_url,
    embedding,
    null,
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
    --, image_details (
    select 
        img.image_url,
        img.image_id,
        img.file_id,
        raw.filename,
        raw.page_number,
        raw.image_base64,
        img.embedding
    from image_embeddings img
    join miguelmoutela.world_inequality_reports_layout_raw raw on (
        raw.file_checksum = img.file_id
        and raw.image_id = img.image_id
    ) 
);