from enum import Enum
from typing import Any
from abc import ABC, abstractmethod


class DatabaseType(Enum):
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"


class BaseDC(ABC):
    "Interface for DatabaseConnection"

    def __init__(self, db_type: DatabaseType):
        self.db_type = db_type

    @abstractmethod
    def connect(self) -> Any:
        pass

    @abstractmethod
    def disconnect(self):
        pass
