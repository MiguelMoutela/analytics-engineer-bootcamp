import os
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

private_key_bytes = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

session = Session.builder.configs({
    "account": os.environ["SF_ACCOUNT"],
    "user": os.environ["SF_USER"],
    "private_key": private_key_bytes,
    "role": os.environ["SF_ROLE"],
    "warehouse": os.environ["SF_WAREHOUSE"],
    "database": os.environ["SF_DATABASE"],
    "schema": os.environ["SF_SCHEMA"]
}).create()

print("result", session.sql("SELECT 1").collect()) 