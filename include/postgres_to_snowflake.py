import pandas as pd
from psycopg2 import sql
from include import dataexpert_postgres as postgres
from include import dataexpert_snowflake as snowflake


def migrate(source: str, target: str):
    """Postgres to Snowflake migration"""

    CHUNK_SIZE = 5000
    has_rows = True
    with postgres.connect() as pg_cx, snowflake.connect() as snow_cx:

        with pg_cx.cursor() as pg_cur:
            pg_cur.execute(
                sql.SQL(
                    "SELECT * FROM {}.{};"
                ).format(
                    *(sql.Identifier(obj) for obj in source.split("."))
                )
            )
            
            if pg_cur.rowcount > 0:
                snow_cx.sql(f"drop table if exists {target}").collect()

                while has_rows:
                    rows = pg_cur.fetchmany(CHUNK_SIZE)
                    df = pd.DataFrame(
                        data=rows, columns=[col.name for col in pg_cur.description]
                    )
                    snow_df = snow_cx.create_dataframe(df)
                    snow_df.write.mode("append").save_as_table(target)
                    has_rows = pg_cur.rownumber != pg_cur.rowcount

    return f"Processed {source} {target}"

