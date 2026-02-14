import os
import json
import queries
import include.dataexpert_snowflake as snowflake

snow_cx = snowflake.get_snowpark_session(schema=os.environ["SF_SCHEMA"])

ai_cx = snow_cx.sql(queries.RAG_RETRIEVE_CONTEXT).collect()
print(f"\nai_cx: {json.dumps(ai_cx[0].as_dict(), indent=2)} " if ai_cx else None)

text_cx = ai_cx[0][0]
text = text_cx.replace("'", "''")

files_cx = json.loads(str(ai_cx[0][1]))
files = json.dumps(files_cx)

ai_response = snow_cx.sql(queries.ai_complete(text, files)).collect()
print(f"\nai_response: {json.dumps(ai_response[0].as_dict(), indent=2)} " if ai_response else None)


