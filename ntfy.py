"""Отправляет уведомления в ntfy с поддержкой прокси и заголовков."""
import time

from loguru import logger
import requests
from returns.result import Result, Success, Failure

def send(config: dict, message: str, tags: str|None = None) -> Result[str, str]:
    """
    Возвращает Success если уведомление отправлено,
    Failure если произошла ошибка при отправке.
    """

    topic = config["topic"]
    if not topic:
        return Failure("В секции [ntfy] не указан 'topic'")
    
    server = config.get("server", "https://ntfy.sh").rstrip("/")
    url = f"{server}/{topic}"
   
    # Заголовки (опционально)
    headers = {}
    title = config.get("title")
    if title:
        headers["Title"] = title
    priority = config.get("priority")
    if priority:
        headers["Priority"] = str(priority)
    if tags:
        headers["Tags"] = tags

    proxies = None
    if "proxy" in config:
        proxies = {"http": config["proxy"], "https": config["proxy"]}
    max_attempts = 5
    attempts = 0
    while attempts < max_attempts:
        try:
            response = requests.post(url,  data=message.encode("utf-8"), headers=headers, proxies=proxies, timeout=10)
            if response.status_code == 200:
                logger.info(f"Уведомление отправлено в ntfy")
                break
            else:
                error_msg = f"ntfy вернул {response.status_code}: {response.text}"
                logger.error(error_msg) 
        except Exception as e:
            error_msg = (f"Ошибка при отправке уведомления: {e}")
            logger.error(error_msg) 
        attempts += 1
        logger.debug(f"Неудача, попытка {attempts}...")
        time.sleep(30)  # Пауза перед следующей попыткой
