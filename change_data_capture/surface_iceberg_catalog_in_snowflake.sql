--eg catalog_id := '7852322649619'

create or replace external volume iceberg_external_volume
    storage_locations =
        (
            (
                name = 'volume name'
                storage_provider = 'S3'
                storage_base_url = 'zachwilsonsorganization-522'
                storage_aws_role_arn = 'arn:aws:iam::catalog_id:role/RoleName'
            )
        )
        allow_writes = false
;

desc external volume iceberg_external_volume;


create or replace catalog integration glue_iceberg_int
    catalog_source = glue
    table_format = iceberg
    glue_aws_role_arn = 'arn:aws:iam::catalog_id:role/RoleName'
    glue_catalog_id = catalog_id
    glue_region = 'us-west-2'
    enabled = true
;

desc integration glue_iceberg_int;


use database bootcamp;
    create or replace iceberg table platform_users
    catalog = 'glue_iceberg_int'
    catalog_namespace = 'zachwilson'
    catalog_table_name = 'platform_users'
    external_volume = 'iceberg_external_volume'
;

select * from platform_users;