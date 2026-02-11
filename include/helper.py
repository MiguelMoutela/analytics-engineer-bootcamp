import tempfile
import hashlib
from pathlib import Path

def calculate_file_checksum(file_path: str) -> str:
    """
    Calculate SHA256 checksum of a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Hexadecimal checksum string
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read file in chunks to handle large files efficiently
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def flush_to_disk(content, filepath):
    with open(filepath, "wb") as f:
        f.write(content)


def save_file(content, filename):
    "meh dont yield - just return?"
    with tempfile.TemporaryDirectory() as temp_dir:
        filepath = Path(temp_dir.name, filename)
        flush_to_disk(content, filepath)
    yield filepath