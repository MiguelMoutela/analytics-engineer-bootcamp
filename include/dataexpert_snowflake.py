import os
import base64
from contextlib import contextmanager
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session

@contextmanager
def connect():
    """Yields session"""

    key_path = os.path.expanduser(os.environ.get("SF_PRIVATE_KEY_PATH"))
    passphrase = os.environ.get("SF_PRIVATE_KEY_PASSPHRASE")
    password_bytes = passphrase.encode() if passphrase else None
    
  
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=password_bytes
        )

    session = Session.builder.configs({
        "account": os.environ["SF_ACCOUNT"],
        "user": os.environ["SF_USER"],
        "private_key": p_key,
        "role": os.environ["SF_ROLE"],
        "warehouse": os.environ["SF_WAREHOUSE"],
        "database": os.environ["SF_DATABASE"],
        "schema": os.environ["SF_SCHEMA"],
    }).create()

    try:
        yield session
    finally:
        session.close()


def execute_sql(sql: str):
    """Creates session and executes sql"""

    with connect() as cx:
        res = cx.sql(sql).collect()
        return res
    return


def has_table(database: str, schema: str, table: str):
    """Check table exists"""
    return len(execute_sql(f"""
        SELECT 1
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table}'
        LIMIT 1
    """)) > 0


def create_view_user_timezone_scd2():
    """Creates scd2 view from audit changes"""

    execute_sql(f"""
        create or replace view user_timezone_scd2 as 
        with user_timezone_changes as ( 
            select "user_id", "new_timezone" as "timezone", "update_time" as "valid_from" 
            from {os.environ["SF_DATABASE"]}.{os.environ["SF_SCHEMA"]}.{os.environ["SF_AUDIT_TABLE"]} 
            union all 
            select "user_id", "timezone", current_timestamp() as "valid_from" 
            from {os.environ["SF_DATABASE"]}.{os.environ["SF_SCHEMA"]}.{os.environ["SF_SNAPSHOT_TABLE"]} )
        , dedup as ( 
            select "user_id", "timezone", "valid_from", lag("timezone") over (partition by "user_id" order by "valid_from") as "prev_timezone" 
            from user_timezone_changes )
        , changes as (
            -- keep the first row and any row where the timezone actually changes 
            select "user_id", "timezone", "valid_from" 
            from dedup where "prev_timezone" is null or "timezone" <> "prev_timezone" )
        , final as ( 
            select "user_id", "timezone", "valid_from", lead("valid_from") over (partition by "user_id" order by "valid_from") as "valid_to" 
            from changes
        ) 
        select "user_id", "timezone", "valid_from", "valid_to", "valid_to" is null as "is_current" 
        from final
    """)




        
