import os
import json
import logging
from typing import Any, Dict
from datetime import datetime


class ColorStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        class AddColor(logging.Formatter):
            def format(self, record: logging.LogRecord):
                msg = super().format(record)
                color = (
                    "\033[1;"
                    + ("32m", "36m", "33m", "31m", "41m")[
                        min(4, int(4 * record.levelno / logging.FATAL))
                    ]
                )
                return color + record.levelname + "\033[1;0m: " + msg

        super().__init__(stream)
        self.setFormatter(AddColor())


class Logger:
    def __init__(self, name: str = "SA_ORM", log_file: str = "log.json"):
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s [%(levelname)s] [%(module)s/%(funcName)s#%(lineno)d] - %(message)s",
            handlers=[ColorStreamHandler()],
            force=True,
        )
        self.logpath = os.path.join(os.getcwd(), log_file)
        self.logger = logging.getLogger(name)

    def log_op(
        self,
        action: str,
        table: str,
        record_id: Any = None,
        success: bool = True,
        metadata: Dict[str, Any] | None = None,
    ):
        log_data = {
            "action": action,
            "resource": table,
            "timestamp": datetime.now().isoformat(),
            "record_id": record_id,
            "success": success,
            "metadata": metadata or {},
        }
        if os.path.isfile(self.logpath) and os.path.getsize(self.logpath) > 0:
            with open(self.logpath, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = []
        else:
            data = []

        data.append(log_data)

        with open(self.logpath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())

    def log(self, message: object, level: str = "debug") -> None:
        severity_methods = {
            "DEBUG": self.logger.debug,
            "INFO": self.logger.info,
            "WARNING": self.logger.warning,
            "ERROR": self.logger.error,
            "CRITICAL": self.logger.critical,
        }
        if level.upper() in severity_methods:
            severity_methods[level.upper()](message)
        else:
            self.logger.info(message)
            self.logger.critical(
                f"Invalid severity level: {level}. Supported levels are: {list(severity_methods.keys())}"
            )
