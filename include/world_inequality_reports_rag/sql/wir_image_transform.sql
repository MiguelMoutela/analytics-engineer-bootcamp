list @dataexpert_student.miguelmoutela.world_inequality_report_images_stage;
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

PROC_BASE64_TO_JPEG = fr""--sql
    CREATE OR REPLACE PROCEDURE DATAEXPERT_STUDENT.MIGUELMOUTELA.EXPORT_WIR_BASE64_TO_STAGE()
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
    AS '
    import base64
    import os

    def run(session):
        # 1. Fetch data from your table
        df = session.table("MIGUELMOUTELA.WORLD_INEQUALITY_REPORTS_LAYOUT_RAW").select("FILE_CHECKSUM", "IMAGE_ID", "IMAGE_BASE64").to_pandas()
        
        stage_path = "@world_inequality_report_images_stage" # Ensure this stage exists
        
        count = 0
        for index, row in df.iterrows():
            file_id = row[''FILE_CHECKSUM'']
            img_id = row[''IMAGE_ID'']
            b64_str = row[''IMAGE_BASE64'']
            
            if not b64_str:
                continue
                
            # 2. Decode Base64 and write to local /tmp
            file_name = f"{file_id}_{img_id}"
            local_path = f"/tmp/{file_name}"
            
            with open(local_path, "wb") as f:
                # guard for data:image/jpeg;base64,/9j/4AAQSkZ...
                if "," in b64_str:
                    b64_str = b64_str.split(",")[1]
                
                f.write(base64.b64decode(b64_str))
            
            # 3. Upload from /tmp to the Snowflake stage
            session.file.put(local_path, stage_path, overwrite=True, auto_compress=False)
            count += 1
            
        return f"Successfully exported {count} images to {stage_path}"
    ';
"""