import os
import queries
from include import dataexpert_snowflake as snowflake

snow_cx = snowflake.get_snowpark_session(os.environ["SF_SCHEMA"])

# snow_cx.sql(queries.CREATE_METADATA_TABLE)

# raw_extract_docs = snow_cx.sql(queries.MERGE_RAW_TRANSFORM).collect()

# t_raw_update_on_complete = snow_cx.sql(queries.update_metadata_as_at("raw_processed_at")).collect()

# process_text_chunks = snow_cx.sql(queries.MERGE_CHUNKS_TRANSFORM).collect()

# t_text_update_on_complete = snow_cx.sql(queries.update_metadata_as_at("text_extracted_at")).collect()

# create_images_stage = snow_cx.sql(queries.create_stage("world_inequality_report_images_stage")).collect()
# proc_decode_and_stage_images = snow_cx.sql(queries.PROC_STAGE_BASE64_TO_JPEG_WIR_IMAGES).collect()

# decode_and_stage_images = snow_cx.call("PROC_STAGE_BASE64_TO_JPEG_WIR_IMAGES")
# t_text_update_on_complete = snow_cx.sql(queries.update_metadata_as_at("images_extracted_at")).collect()
                                                  
images_staged = snow_cx.sql(queries.list_stage("world_inequality_report_images_stage")).collect()
images_processed_set = {row["IMAGE_URL"] for row in snow_cx.table("world_inequality_images").select("image_url").collect()}

new_imagessdkjh = [row for row in images_staged if row["image_url"] not in images_processed_set]

if new_images:
    list_values = [
        (
            row['name'],                              
            os.path.basename(row['name']),            
            os.path.basename(row['name']).split('_')[0], 
            os.path.basename(row['name']).split('_')[1] 
        ) 
        for row in new_images
    ]

process_images = snow_cx.sql(queries.merge_images_transform(list_values)).collect()
print('hello')
