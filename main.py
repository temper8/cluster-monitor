#!/usr/bin/env python3
"""
main.py

Оркестратор для сбора состояния Slurm кластера.
Загружает конфиг, получает данные через SSH, проверяет изменения,
сохраняет результат, парсит вывод и отправляет уведомления в ntfy.
"""

import sys
from pathlib import Path
from datetime import datetime
from loguru import logger

# Импорты модулей проекта
from logger_setup import setup_logging
import slurm_collector
import hash_utils
import slurm_parser
import notifier

CONFIG_PATH = Path(__file__).parent / "config.toml"

def load_config() -> dict:
    """Загружает конфигурацию из TOML-файла."""
    import tomllib
    if not CONFIG_PATH.exists():
        logger.error(f"Файл конфигурации не найден: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)

def should_save_output(stdout: str, hash_path: Path | None) -> bool:
    """
    Определяет, нужно ли сохранять вывод.
    Если hash_path не задан → всегда сохранять.
    Если задан и хэш изменился → сохранять, иначе нет.
    """
    if hash_path is None:
        logger.debug("hash_file не задан, сохраняем всегда")
        return True
    # нужно удалить первую строку - в ней содержиться время.
    current_hash = hash_utils.compute_hash(stdout.split('\n', 1)[-1])
    previous_hash = hash_utils.load_previous_hash(hash_path)
    if previous_hash is not None and current_hash == previous_hash:
        logger.info("Хэш вывода sinfo не изменился. Сохранение не требуется.")
        return False

    logger.info(f"Хэш изменился (был {previous_hash[:8] if previous_hash else 'None'}, "
                f"стал {current_hash[:8]}...). Сохраняем.")
    if hash_path:
        hash_utils.save_hash(hash_path, current_hash)
        logger.debug(f"Хэш сохранён в {hash_path}")
    return True

def save_output_with_header(stdout: str, output_file: str, host: str, sinfo_args: str) -> None:
    """Добавляет временной заголовок и сохраняет вывод в файл."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = (
        f"# Состояние Slurm кластера от {timestamp}\n"
        f"# Хост: {host}\n"
        f"# Команда: sinfo {sinfo_args}\n\n"
    )
    content = header + stdout
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info(f"Результат сохранён в {output_path}")

def main() -> None:
    # 1. Настройка логирования
    setup_logging()

    logger.info("Запуск оркестратора cluster_monitor")

    # 2. Загрузка конфигурации
    full_config = load_config()
    slurm_cfg = full_config.get("slurm")
    ntfy_cfg = full_config.get("ntfy")

    if not slurm_cfg:
        logger.error("В config.toml отсутствует секция [slurm]")
        sys.exit(1)

    # 3. Получение вывода sinfo через SSH
    try:
        stdout, stderr, rc = slurm_collector.fetch_sinfo(slurm_cfg)
    except Exception as e:
        logger.exception(f"Ошибка при вызове fetch_sinfo: {e}")
        sys.exit(1)

    if rc != 0:
        logger.error(f"Ошибка выполнения sinfo (код {rc})")
        if stderr:
            logger.error(f"stderr: {stderr}")
        sys.exit(1)

    if not stdout.strip():
        logger.warning("sinfo не вернул данных (возможно, пустой кластер)")

    # 4. Определение путей из конфига
    output_file = slurm_cfg.get("output_file")
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"slurm_state_{timestamp}.txt"
        logger.info(f"output_file не задан, используется {output_file}")

    hash_file = slurm_cfg.get("hash_file")
    hash_path = Path(hash_file) if hash_file else None

    # 5. Проверка изменений и сохранение
    send_notification = should_save_output(stdout, hash_path)
    if send_notification:
        save_output_with_header(stdout, output_file,
                                slurm_cfg['host'],
                                slurm_cfg.get('sinfo_args', ''))


    # 6. Парсинг вывода sinfo
    try:
        parsed = slurm_parser.parse_sinfo_output(stdout)
    except Exception as e:
        logger.exception(f"Ошибка парсинга вывода sinfo: {e}")
        sys.exit(1)

    free_nodes = slurm_parser.count_free_nodes(parsed)
    logger.info(f"Обнаружено свободных узлов: {free_nodes}")

    # 7. Отправка уведомления в ntfy (если секция ntfy присутствует)
    if ntfy_cfg:
        logger.info(f"Свободных узлов ({free_nodes}) ")
        if send_notification:
            message = ntfy_cfg.get("message", f"В кластере {free_nodes} свободных узлов")
            title = ntfy_cfg.get("title", "Состояние кластера Slurm")
            try:
                success = notifier.send_ntfy_notification(
                    message=message,
                    ntfy_config=ntfy_cfg,
                    free_nodes=free_nodes  # можно передать для форматирования
                )
                if success:
                    logger.info("Уведомление в ntfy отправлено")
                else:
                    logger.warning("Не удалось отправить уведомление в ntfy")
            except Exception as e:
                logger.exception(f"Ошибка при отправке уведомления: {e}")
    else:
        logger.debug("Секция [ntfy] отсутствует, уведомления не отправляются")

    logger.success("Оркестратор завершил работу")

if __name__ == "__main__":
    main()