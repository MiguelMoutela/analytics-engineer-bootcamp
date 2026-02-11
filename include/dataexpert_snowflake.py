import os
import base64
from contextlib import contextmanager
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session

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


@contextmanager
def connect():

    # private_key_bytes = p_key.private_bytes(
    #     encoding=serialization.Encoding.DER,
    #     format=serialization.PrivateFormat.PKCS8,
    #     encryption_algorithm=serialization.NoEncryption(),
    # )

    # private_key = serialization.load_pem_private_key(
    #     base64.b64decode(os.environ["SF_PRIVATE_KEY_BASE64"]),
    #     password=os.environ["SF_PRIVATE_KEY_PASSPHRASE"].encode(),
    # )

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


def create_stage(database: str, schema: str, stage_name: str):
    """Create stage"""
    return execute_sql(f"""
        CREATE STAGE IF NOT EXISTS {database}.{schema}.{stage_name}
        ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
        COMMENT = 'Stage for World Inequality reports'
    """)


def stage_upload(stage_name: str, file_name: str, file_path: str):
    return execute_sql(f"""
        PUT file://{file_path} @{stage_name}/{file_name}/ AUTO_COMPRESS=FALSE
    """)


def create_view_user_timezone_scd2():
    """Creates scd2 view from audit changes"""
    
    db = os.environ["SF_DATABASE"] 
    sc = os.environ["SF_SCHEMA"] 
    audit_tbl = os.environ["SF_AUDIT_TABLE"] 
    snap_tbl = os.environ["SF_SNAPSHOT_TABLE"]
    snowflake_cap = "to_timestamp_ntz('9999-12-31 23:59:59')"

    execute_sql(f"""
        create or replace view user_timezone_scd2 as
        with audit as (
            select "user_id", "new_timezone" as "timezone", "update_time" as "valid_from"
            from {db}.{sc}.{audit_tbl}
        ),
        cap as (
            -- cap one row per user with the current timezone, used only to compute valid_to for the last change
            select s."user_id", s."timezone", {snowflake_cap} as "valid_from"
            from {db}.{sc}.{snap_tbl} s
        ),
        events as (
            select * from audit
            union all
            select * from cap
        ),
        changes as (
            -- remove consecutive duplicates
            select
                "user_id",
                "timezone",
                "valid_from",
                lag("timezone") over (partition by "user_id" order by "valid_from") as "prev_timezone"
            from events
        ),
        dedup as (
            select "user_id","timezone","valid_from"
            from changes
            where "prev_timezone" is null or "timezone" <> "prev_timezone"
        ),
        final as (
            select
                "user_id",
                "timezone",
                "valid_from",
                lead("valid_from") over (partition by "user_id" order by "valid_from") as "valid_to_raw"
            from dedup
        )
        select
            "user_id",
            "timezone",
            "valid_from",
            case when "valid_to_raw" = {snowflake_cap} then null else "valid_to_raw" end as "valid_to",
            case when "valid_to_raw" = {snowflake_cap} then true else false end as "is_current"
        from final
    """)



        


def get_snowpark_session(schema: str):
    return session 