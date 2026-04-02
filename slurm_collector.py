#!/usr/bin/env python3
"""
slurm_collector.py

Собирает состояние кластера Slurm через sinfo по SSH и сохраняет в файл.
Параметры берутся из config.toml (секция [slurm]).
Использует paramiko для SSH-подключения.
"""

import sys
import os
from datetime import datetime
from pathlib import Path
import paramiko

# Поддержка TOML для разных версий Python
try:
    from tomllib import load as toml_load
except ImportError:
    try:
        from tomli import load as toml_load
    except ImportError:
        print("Ошибка: требуется модуль tomli (Python<3.11) или tomllib (3.11+). Установите: pip install tomli", file=sys.stderr)
        sys.exit(1)

CONFIG_PATH = Path(__file__).parent / "config.toml"

def load_config():
    """Загружает конфигурацию из TOML-файла."""
    if not CONFIG_PATH.exists():
        print(f"Файл конфигурации не найден: {CONFIG_PATH}", file=sys.stderr)
        sys.exit(1)
    with open(CONFIG_PATH, "rb") as f:
        return toml_load(f)

def create_ssh_client(config):
    """Создаёт и возвращает SSH-клиент paramiko на основе конфига."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    host = config.get("host")
    if not host:
        raise ValueError("В конфиге отсутствует 'host' в секции [slurm]")

    username = config.get("username")
    port = config.get("port", 22)
    timeout = config.get("timeout", 10)

    key_path = config.get("key_path")
    password = config.get("password")

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

    client.connect(**connect_kwargs)
    return client

def run_sinfo(client, sinfo_args):
    """Выполняет команду sinfo и возвращает stdout, stderr, exit code."""
    command = f"sinfo {sinfo_args}".strip()
    stdin, stdout, stderr = client.exec_command(command, timeout=30)
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode("utf-8")
    err = stderr.read().decode("utf-8")
    return out, err, exit_code

def save_output(content, output_file):
    """Сохраняет содержимое в файл."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"Ошибка записи в файл {output_path}: {e}", file=sys.stderr)
        return False

def main():
    # Загрузка конфигурации
    full_config = load_config()
    slurm_cfg = full_config.get("slurm", {})
    if not slurm_cfg:
        print("Ошибка: в config.toml отсутствует секция [slurm]", file=sys.stderr)
        sys.exit(1)

    # Параметры с значениями по умолчанию
    sinfo_args = slurm_cfg.get("sinfo_args", "-o '%P %a %l %D %T %c %m %e'")
    output_file = slurm_cfg.get("output_file")
    append = slurm_cfg.get("append", False)

    # Если output_file не указан, генерируем с датой
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"slurm_state_{timestamp}.txt"

    # Подключение по SSH и выполнение sinfo
    client = None
    try:
        print(f"Подключение к {slurm_cfg.get('host')}...")
        client = create_ssh_client(slurm_cfg)
        print("Выполнение sinfo...")
        stdout, stderr, rc = run_sinfo(client, sinfo_args)

        if rc != 0:
            print(f"Ошибка выполнения sinfo (код {rc}):", file=sys.stderr)
            print(stderr, file=sys.stderr)
            sys.exit(1)

        if not stdout.strip():
            print("Предупреждение: sinfo не вернул данных (возможно, пустой кластер).", file=sys.stderr)

        # Формируем вывод с заголовком
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"# Состояние Slurm кластера от {timestamp}\n# Хост: {slurm_cfg['host']}\n# Команда: sinfo {sinfo_args}\n\n"
        content = header + stdout

        if save_output(content, output_file, append):
            print(f"Результат сохранён в {output_file}")
        else:
            sys.exit(1)

    except Exception as e:
        print(f"Критическая ошибка: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()