import os
import json
import queries
from include import dataexpert_snowflake as snowflake

snow_cx = snowflake.get_snowpark_session(os.environ["SF_SCHEMA"])

created_extract_table = snow_cx.sql(queries.CREATE_RAW_EXTRACT_TABLE).collect()
print(f"\ncreated_extract_table: {json.dumps(created_extract_table[0].as_dict(), indent=2)} " if created_extract_table else None) 

raw_extract_docs = snow_cx.sql(queries.MERGE_RAW_TRANSFORM).collect()
print(f"\nraw_extract_docs: {json.dumps(raw_extract_docs[0].as_dict(), indent=2)} " if raw_extract_docs else None) 

update_raw_step_on_complete = snow_cx.sql(queries.update_metadata_on_raw_extract_complete()).collect()
print(f"\nupdate_raw_step_on_complete: {json.dumps(update_raw_step_on_complete[0].as_dict(), indent=2)} " if update_raw_step_on_complete else None)

created_chunks_table = snow_cx.sql(queries.CREATE_CHUNKS_TABLE).collect()
print(f"\nreated_chunks_table: {json.dumps(created_chunks_table[0].as_dict(), indent=2)} " if created_chunks_table else None)

process_text_chunks = snow_cx.sql(queries.MERGE_CHUNKS_TRANSFORM).collect()
print(f"\nprocess_text_chunks: {json.dumps(process_text_chunks[0].as_dict(), indent=2)} " if process_text_chunks else None)

update_text_step_on_complete = snow_cx.sql(queries.update_metadata_on_text_proc_complete()).collect()
print(f"\nupdate_text_step_on_complete: {json.dumps(update_text_step_on_complete[0].as_dict(), indent=2)} " if update_text_step_on_complete else None)

create_images_stage = snow_cx.sql(
    queries.create_stage("world_inequality_reports_images_stage"), params=["Stage for Images of World Inequality Reports"]
).collect()
print(f"\ncreate_images_stage: {json.dumps(create_images_stage[0].as_dict(), indent=2)} " if create_images_stage else None)

create_proc_decode_and_stage_images = snow_cx.sql(queries.PROC_STAGE_BASE64_TO_JPEG_WIR_IMAGES).collect()
print(f"\ncreate_proc_decode_and_stage_images: {json.dumps(create_proc_decode_and_stage_images[0].as_dict(), indent=2)} " if create_proc_decode_and_stage_images else None)

call_decode_and_stage_images = snow_cx.call("PROC_STAGE_BASE64_TO_JPEG_WIR_IMAGES")
print("\ncall_decode_and_stage_images ", call_decode_and_stage_images)

update_images_step_on_complete = snow_cx.sql(queries.update_metadata_on_image_proc_complete()).collect()
print(f"\nupdate_images_step_on_complete: {json.dumps(update_images_step_on_complete[0].as_dict(), indent=2)} " if update_images_step_on_complete else None)

images_staged = snow_cx.sql(queries.list_stage("world_inequality_reports_images_stage")).collect()
print(f"\nimages_staged: {json.dumps(images_staged[0].as_dict(), indent=2)} " if images_staged else None)

created_images_table = snow_cx.sql(queries.CREATE_IMAGES_TABLE).collect()
print(f"\ncreated_images_table: {json.dumps(created_images_table[0].as_dict(), indent=2)} " if created_images_table else None)

images_processed_set = {row["IMAGE_URL"] for row in snow_cx.table("world_inequality_reports_images").select("image_url").collect()}
print("\nimages_processed_set ", len(images_processed_set))

new_images = [row for row in images_staged if row["name"] not in images_processed_set]
print("\nnew_images ", len(new_images))

list_values = [
    (
        row['name'],  
        f"@{os.path.dirname(str(row['name']))}",
        os.path.basename(str(row['name'])),            
        os.path.basename(str(row['name'])).split('_')[0], 
        os.path.basename(str(row['name'])).split('_')[1] 
    ) 
    for row in new_images
]

# build string representations of `rows` image1 .. imageN to be used as SQL `SELECT * FROM rows`
# image1 eg ('snowflake-stage-url-image1.jpeg', 'snowflake-stage-name', 'file-id-image1.jpeg, 'file_id', 'image1.jpeg')
list_values_str = ", ".join([f"('{r[0]}','{r[1]}', '{r[2]}', '{r[3]}', '{r[4]}')" for r in list_values])
null_list_values_str = "('NULL-IMAGE-FULLNAME','NULL-STAGE-NAME','NULL-IMAGE-ID','NULL-FILEID','NULL-IMAGE-NUMBER')"
values_str = list_values_str or null_list_values_str

process_images = snow_cx.sql(queries.merge_images_transform(values_str)).collect() 
print(f"\nprocess_images: {json.dumps(process_images[0].as_dict(), indent=2)} " if process_images else None)

update_images_step_on_complete = snow_cx.sql(queries.update_metadata_on_image_extract_complete()).collect()
print(f"\nupdate_images_step_on_complete: {json.dumps(update_images_step_on_complete[0].as_dict(), indent=2)} " if update_images_step_on_complete else None)



