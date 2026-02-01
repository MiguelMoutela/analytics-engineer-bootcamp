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
