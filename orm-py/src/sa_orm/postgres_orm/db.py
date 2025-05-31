import psycopg
from typing import Any
from base.declare import BaseDC, DatabaseType
from log import Logger

Log = Logger()
log = Log.log
dump = Log.log_op


class DatabaseConnection(BaseDC):
    """Manages PostgreSQL database connections using psycopg3"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "password",
    ):
        super().__init__(DatabaseType.POSTGRESQL)
        self.connection_params = {
            "host": host,
            "port": port,
            "dbname": database,
            "user": user,
            "password": password,
        }
        self._connection = None

    def connect(self) -> Any:
        try:
            self._connection = psycopg.connect(**self.connection_params)
            self._connection.autocommit = False
            log(
                f"Connected to PostgreSQL database: {self.connection_params['dbname']}",
                "DEBUG",
            )
            log(
                f"=== Testing Database Connection @{self.connection_params['dbname']} ===",
                "DEBUG",
            )
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()
                log(f"PostgreSQL version: {version[0]}", "DEBUG")

            return self._connection
        except Exception as e:
            log(f"Error connecting to database: {e}", "ERROR")
            raise

    def disconnect(self):
        if self._connection:
            self._connection.close()
            log(
                f"{self.connection_params['host']}:{self.connection_params['port']}@{self.connection_params['dbname']} Database connection closed",
                "DEBUG",
            )

    @property
    def connection(self):
        if not self._connection or self._connection.closed:
            self.connect()
        return self._connection

    def __repr__(self) -> str:
        return f"{self.connection_params['host']}:{self.connection_params['port']}@{self.connection_params['dbname']}"
