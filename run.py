import sys
import tomllib
from pathlib import Path

from returns.pipeline import flow, is_successful
from returns.pointfree import bind, map_
from loguru import logger
from returns.result import Failure, Success

from logger_setup import setup_logging
from monitor import SlurmMonitor
import ntfy


def load_config(config_path: Path) -> dict:
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def main():
    base_dir = Path(__file__).parent
    config_file = base_dir / "config.toml"
    output_dir = base_dir / "data"

    if not config_file.exists():
        print(f"Ошибка: config.toml не найден в {config_file}")
        sys.exit(1)

    config = load_config(config_file)
    setup_logging() #config.get("logging", {}))

    slurm_cfg = config.get("slurm")
    if not slurm_cfg:
        logger.error("Отсутствует секция [slurm] в конфиге")
        sys.exit(1)

    monitor = SlurmMonitor(slurm_cfg, output_dir)
    
    content = monitor.fetch_output()
    changed = content.bind(monitor.save_if_changed).value_or(False)
    result  = content.bind(monitor.parse_states)
    #changed = True
    # Обработка результата парсинга
    if is_successful(result):
         free, allocated, down = result.unwrap()
         logger.info(f"Состояние кластера {'' if changed else "не"} изменилось.")
         logger.info(f"Состояние кластера: free={free}, allocated={allocated}, down={down}")
         if changed:
            ntfy_cfg = config.get("ntfy")
            if ntfy_cfg:
                tags = "red_circle" if free == 0 else "green_circle"
                res = ntfy.send(ntfy_cfg,  message = f"free={free}, allocated={allocated}, down={down}", tags= tags )
     
    else:
        logger.error(f"Мониторинг не удался: {result.failure()}")
    logger.info("=== Мониторинг завершён ===")
if __name__ == "__main__":
    main()