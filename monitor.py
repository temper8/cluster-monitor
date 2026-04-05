import io
from pathlib import Path

import pandas as pd
import paramiko
from loguru import logger
from returns.result import Result, ResultE, Success, Failure, safe
from returns.pipeline import flow, is_successful
from returns.pointfree import bind

from utils import compute_sha256, read_hash, write_hash


class SlurmMonitor:
    """Отвечает только за подключение к Slurm, выполнение sinfo, сохранение и парсинг."""

    def __init__(self, slurm_config: dict, output_dir: Path):
        self.slurm_config = slurm_config
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)

        self.output_file = output_dir / "sinfo_output.txt"
        self.hash_file = output_dir / "sinfo_output.hash"

    @safe
    def _create_ssh_client(self) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client

    def fetch_output(self) -> Result[str, Exception]:
        """Выполняет sinfo -N -l на удалённом хосте. Возвращает Result[str, Exception]."""
        host = self.slurm_config["host"]
        username = self.slurm_config["username"]

        @safe
        def execute(client: paramiko.SSHClient) -> str:
            connect_kwargs = {
                "hostname": host,
                "username": username,
            }
            if "key_filename" in self.slurm_config:
                connect_kwargs["key_filename"] = self.slurm_config["key_filename"]
            if "password" in self.slurm_config:
                connect_kwargs["password"] = self.slurm_config["password"]

            client.connect(**connect_kwargs)
            logger.info(f"Подключено к {host} как {username}")

            _, stdout, stderr = client.exec_command("sinfo -N -l")
            output = stdout.read().decode("utf-8")
            error = stderr.read().decode("utf-8")
            return output

        # Цепочка: создаём клиент -> выполняем команду -> закрываем клиент
        return self._create_ssh_client().bind(lambda client: execute(client))

    @safe
    def save_if_changed(self, content: str) -> bool:
        """Сохраняет вывод, если изменился хэш. Возвращает Result[bool, Exception]."""
        new_hash = compute_sha256(content.split('\n', 1)[-1])
        old_hash = read_hash(self.hash_file)
        if new_hash == old_hash:
            logger.debug("Содержимое не изменилось, файл не перезаписан")
            return False
        self.output_file.write_text(content, encoding="utf-8")
        write_hash(self.hash_file, new_hash)
        logger.info(f"Вывод сохранён в {self.output_file} (хэш: {new_hash[:8]}...)")
        return True

    @safe
    def parse_states(self, raw_output: str) -> tuple[int, int, int]:
        """
        Парсит вывод sinfo -N -l с помощью pandas.read_fwf.
        Возвращает словарь с df, free_nodes, allocated_nodes, down_nodes,
        total_nodes, states, nodes.
        """
        df = pd.read_fwf(io.StringIO(raw_output), header=1)

        # Удаляем звёздочку из состояний (например, down* -> down)
        df['STATE'] = df['STATE'].str.rstrip('*')
        #df['NODES'] = df['NODES'].str.strip()

        # Подсчёт узлов по состояниям
        states_counts = df['STATE'].value_counts()
        #print(states_counts)

        free = states_counts.get('idle',0)
        allocated = states_counts.get('allocated',0)
        down = states_counts.get('down',0)
        logger.info(f"Парсинг завершён: free={free}, allocated={allocated}, down={down}")
        return free, allocated, down