import os
import queries
from include import dataexpert_snowflake as snowflake

snow_cx = snowflake.get_snowpark_session(os.environ["SF_SCHEMA"])

created_extract_table = snow_cx.sql(queries.CREATE_RAW_EXTRACT_TABLE).collect()

raw_extract_docs = snow_cx.sql(queries.MERGE_RAW_TRANSFORM).collect()
update_raw_step_on_complete = snow_cx.sql(
    queries.update_metadata_as_at(
        "raw_processed_at", 
        queries.last_updated_ts("world_inequality_reports_layout_raw")
    )
).collect()

process_text_chunks = snow_cx.sql(queries.MERGE_CHUNKS_TRANSFORM).collect()
update_text_step_on_complete = snow_cx.sql(
    queries.update_metadata_as_at(
        "text_extracted_at", 
        queries.last_updated_ts("world_inequality_reports_text_chunks")
    )
).collect()

create_images_stage = snow_cx.sql(queries.create_stage("world_inequality_report_images_stage")).collect()
create_proc_decode_and_stage_images = snow_cx.sql(queries.PROC_STAGE_BASE64_TO_JPEG_WIR_IMAGES).collect()

call_decode_and_stage_images = snow_cx.call("PROC_STAGE_BASE64_TO_JPEG_WIR_IMAGES")
update_images_step_on_complete = snow_cx.sql(
    queries.update_metadata_as_at(
        "images_extracted_at", 
        queries.last_updated_ts("world_inequality_report_images")
    )
).collect()              

images_staged = snow_cx.sql(queries.list_stage("world_inequality_report_images_stage")).collect()
images_processed_set = {row["IMAGE_URL"] for row in snow_cx.table("world_inequality_images").select("image_url").collect()}

new_images = [row for row in images_staged if row["name"] not in images_processed_set]

if new_images:
    list_values = [
        (
            row['name'],                              
            os.path.basename(str(row['name'])),            
            os.path.basename(str(row['name'])).split('_')[0], 
            os.path.basename(str(row['name'])).split('_')[1] 
        ) 
        for row in new_images
    ]

    list_values_str = ", ".join([f"('{r[0]}', '{r[1]}', '{r[2]}', '{r[3]}')" for r in list_values])
    process_images = snow_cx.sql(queries.merge_images_transform(list_values_str)).collect()

print('hello')
