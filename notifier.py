#!/usr/bin/env python3
"""
notifier.py

Отправка уведомлений в ntfy (и другие сервисы, например Telegram).
Использует библиотеку requests.
"""

import requests
from loguru import logger

def send_ntfy_notification(message: str, ntfy_config: dict, free_nodes: int = None) -> bool:
    """
    Отправляет уведомление в ntfy.

    Аргументы:
        message: текст сообщения (может быть шаблоном, если в конфиге есть 'message' с {free_nodes})
        ntfy_config: словарь из секции [ntfy] конфига
        free_nodes: количество свободных узлов (для подстановки в шаблон сообщения)

    Возвращает:
        True при успешной отправке, False при ошибке
    """
    # Получаем параметры из конфига
    topic = ntfy_config.get("topic")
    if not topic:
        logger.error("В секции [ntfy] не указан 'topic'")
        return False

    server = ntfy_config.get("server", "https://ntfy.sh").rstrip("/")
    url = f"{server}/{topic}"

    # Формируем сообщение: если указан шаблон в конфиге и есть free_nodes – подставляем
    message_template = ntfy_config.get("message", message)
    if free_nodes is not None and "{free_nodes}" in message_template:
        final_message = message_template.format(free_nodes=free_nodes)
    else:
        final_message = message_template

    # Заголовки (опционально)
    headers = {}
    title = ntfy_config.get("title")
    if title:
        headers["Title"] = title
    priority = ntfy_config.get("priority")
    if priority:
        headers["Priority"] = str(priority)
    tags = ntfy_config.get("tags")
    if tags:
        headers["Tags"] = tags
    # Дополнительные заголовки из конфига (префикс 'header_')
    for key, value in ntfy_config.items():
        if key.startswith("header_"):
            header_name = key[7:]  # убираем 'header_'
            headers[header_name] = str(value)
    
    # Прокси
    proxies = None
    proxy_url = ntfy_config.get("proxy")
    if proxy_url:
        proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        logger.debug(f"Используется прокси: {proxy_url}")

    try:
        response = requests.post(url, data=final_message.encode("utf-8"), headers=headers, timeout=10, proxies=proxies)
        response.raise_for_status()
        logger.info(f"Уведомление отправлено в ntfy: {url}")
        logger.debug(f"Сообщение: {final_message}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка отправки в ntfy: {e}")
        return False

def send_telegram_notification(message: str, telegram_config: dict) -> bool:
    """
    Заготовка для отправки в Telegram (пока не реализована).
    """
    logger.warning("Telegram уведомления ещё не реализованы")
    return False