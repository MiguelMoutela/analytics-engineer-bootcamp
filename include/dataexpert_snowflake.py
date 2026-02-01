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


def has_table(table: str):
    """Check table exists"""
    return len(execute_sql(f"""
        SELECT 1
        FROM {os.environ["SF_DATABASE"]}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{os.environ["SF_SCHEMA"]}'
        AND TABLE_NAME = '{table}'
        LIMIT 1
    """)) > 0


def create_view_user_timezone_scd2():
    """Creates scd2 view from audit changes"""

    execute_sql("""
        create or replace view user_timezone_scd2 as
        with user_timezone_changes as (
            select 
                "user_id", 
                "old_timezone" as "timezone", 
                "update_time" as "valid_from",
                lead("update_time") over (
                    partition by "user_id"
                    order by "update_time" asc
                ) as "valid_to"
            from miguelmoutela.user_timezone_audit_raw
            union all
            select 
                "user_id", 
                "timezone", 
                "update_time" as "valid_from",
                current_timestamp() AS "valid_from"
            from user_timezone_snapshot_raw s
            where not exists (
                select 1
                from user_timezone_audit_raw a
                where a."user_id" = s."user_id"
            )
        )
        select *,
            "valid_to" is null as is_current,
            row_number() over (
                partition by "user_id"
                order by 
                "valid_to" desc nulls first
            ) as "rn"
        from user_timezone_changes
    """)




        
