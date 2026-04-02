
import sys
from loguru import logger

def setup_logging(log_file: str = "slurm_collector.log") -> None:
    """Настраивает логирование loguru: вывод в терминал и в файл."""
    logger.remove()  # Удаляем стандартный обработчик (по умолчанию stderr)
    # Вывод в терминал (stderr) с цветом и форматом
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    # Вывод в файл с ротацией (10 МБ, хранить 3 файла)
    logger.add(
        log_file,
        rotation="10 MB",
        retention=3,
        compression="gz",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
        level="DEBUG"
    )