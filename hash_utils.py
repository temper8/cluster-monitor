#!/usr/bin/env python3
"""
hash_utils.py

Утилиты для работы с хэшами (SHA256).
Используется для отслеживания изменений вывода sinfo.
"""

import hashlib
from pathlib import Path

def compute_hash(data: str) -> str:
    """
    Вычисляет SHA256 хэш от строки.

    Аргументы:
        data: входная строка (например, вывод sinfo)

    Возвращает:
        hex-строку хэша
    """
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def load_previous_hash(hash_path: Path) -> str | None:
    """
    Загружает сохранённый хэш из файла.

    Аргументы:
        hash_path: путь к файлу с хэшем

    Возвращает:
        строку с хэшем или None, если файл не существует
    """
    if hash_path.exists():
        return hash_path.read_text(encoding="utf-8").strip()
    return None

def save_hash(hash_path: Path, hash_value: str) -> None:
    """
    Сохраняет хэш в файл (создавая промежуточные директории при необходимости).

    Аргументы:
        hash_path: путь к файлу для сохранения
        hash_value: строка хэша
    """
    hash_path.parent.mkdir(parents=True, exist_ok=True)
    hash_path.write_text(hash_value, encoding="utf-8")