import hashlib
from pathlib import Path

def compute_sha256(content: str) -> str:
    """Вычисление SHA256 хэша строки."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def read_hash(hash_file: Path) -> str | None:
    """Чтение сохранённого хэша из файла."""
    if hash_file.exists():
        return hash_file.read_text(encoding="utf-8").strip()
    return None

def write_hash(hash_file: Path, hash_value: str) -> None:
    """Запись хэша в файл."""
    hash_file.write_text(hash_value, encoding="utf-8")