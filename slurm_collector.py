#!/usr/bin/env python3
"""
slurm_collector.py

Собирает состояние кластера Slurm через sinfo по SSH и сохраняет в файл.
Параметры берутся из config.toml (секция [slurm]).
Требуется Python 3.13+.
"""

import sys
import os
from datetime import datetime
from pathlib import Path
import tomllib
import paramiko
from loguru import logger

CONFIG_PATH = Path(__file__).parent / "config.toml"


def load_config() -> dict:
    """Загружает конфигурацию из TOML-файла."""
    if not CONFIG_PATH.exists():
        logger.error(f"Файл конфигурации не найден: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)

def fetch_sinfo(slurm_config: dict) -> tuple[str, str, int]:
    """
    Подключается по SSH к удалённому хосту, выполняет sinfo и возвращает вывод.

    Аргументы:
        slurm_config: секция [slurm] из config.toml

    Возвращает:
        (stdout, stderr, exit_code)
    """
    host = slurm_config.get("host")
    if not host:
        raise ValueError("В конфиге отсутствует 'host' в секции [slurm]")

    username = slurm_config.get("username")
    port = slurm_config.get("port", 22)
    timeout = slurm_config.get("timeout", 10)
    key_path = slurm_config.get("key_path")
    password = slurm_config.get("password")
    sinfo_args = slurm_config.get("sinfo_args", "-o '%P %a %l %D %T %c %m %e'")

    connect_kwargs = {
        "hostname": host,
        "port": port,
        "username": username,
        "timeout": timeout,
    }
    if key_path:
        expanded_path = os.path.expanduser(key_path)
        if not os.path.exists(expanded_path):
            raise FileNotFoundError(f"SSH-ключ не найден: {expanded_path}")
        connect_kwargs["key_filename"] = expanded_path
    elif password:
        connect_kwargs["password"] = password
    #else:
    #    raise ValueError("В конфиге должен быть указан key_path или password для SSH")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        logger.info(f"Подключение к {host}:{port} как {username}")
        client.connect(**connect_kwargs)
        command = f"sinfo {sinfo_args}".strip()
        logger.debug(f"Выполняется команда: {command}")
        _, stdout, stderr = client.exec_command(command, timeout=30)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8")
        err = stderr.read().decode("utf-8")
        logger.info(f"Команда завершена с кодом {exit_code}")
        return out, err, exit_code
    except Exception as e:
        logger.exception(f"Ошибка при выполнении SSH/команды: {e}")
        raise
    finally:
        client.close()

def save_output(content: str, output_file: str) -> None:
    """Сохраняет содержимое в файл (с перезаписью)."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info(f"Результат сохранён в {output_path}")

def main() -> None:

    logger.info("Запуск slurm_collector.py")

    full_config = load_config()
    slurm_cfg = full_config.get("slurm")
    if not slurm_cfg:
        logger.error("В config.toml отсутствует секция [slurm]")
        sys.exit(1)

    output_file = slurm_cfg.get("output_file")
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"slurm_state_{timestamp}.txt"
        logger.info(f"output_file не задан, используется {output_file}")

    try:
        stdout, stderr, rc = fetch_sinfo(slurm_cfg)

        if rc != 0:
            logger.error(f"Ошибка выполнения sinfo (код {rc})")
            logger.error(f"stderr: {stderr}")
            sys.exit(1)

        if not stdout.strip():
            logger.warning("sinfo не вернул данных (возможно, пустой кластер)")

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = (
            f"# Состояние Slurm кластера от {timestamp}\n"
            f"# Хост: {slurm_cfg['host']}\n"
            f"# Команда: sinfo {slurm_cfg.get('sinfo_args', '')}\n\n"
        )
        content = header + stdout

        save_output(content, output_file)
        logger.success("Скрипт успешно завершён")
    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()  