from typing import Any
from mysql.connector import connect
from mysql.connector import Error
from ..base.declare import BaseDC, DatabaseType
from ..log import Logger

Log = Logger()
log = Log.log
log_op = Log.log_op


class DatabaseConnection(BaseDC):
    """Manages MySQL database connections using mysql-connector-python"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        database: str = "mysql",
        user: str = "root",
        password: str = "password",
    ):
        super().__init__(DatabaseType.MYSQL)
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
        self._connection = None

    def connect(self) -> Any:
        try:
            self._connection = connect(**self.connection_params)
            if self._connection.is_connected():
                log(
                    f"Connected to MySQL database: {self.connection_params['database']}"
                )
                return self._connection
            else:
                raise Error("Failed to connect to MySQL database", "ERROR")
        except Error as e:
            log(f"Error connecting to database: {e}", "ERROR")
            raise

    def disconnect(self):
        if self._connection and self._connection.is_connected():
            self._connection.close()
            log(
                f"{self.connection_params['host']}:{self.connection_params['port']}@{self.connection_params['database']} Database connection closed"
            )

    @property
    def connection(self):
        if not self._connection or not self._connection.is_connected():
            self.connect()
        return self._connection

    def __repr__(self) -> str:
        return f"{self.connection_params['host']}:{self.connection_params['port']}@{self.connection_params['database']}"
