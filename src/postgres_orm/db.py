import psycopg


class DatabaseConnection:
    """Manages PostgreSQL database connections using psycopg3"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "password",
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "dbname": database,
            "user": user,
            "password": password,
        }
        self._connection = None

    def connect(self):
        try:
            self._connection = psycopg.connect(**self.connection_params)
            self._connection.autocommit = False
            print(
                f"Connected to PostgreSQL database: {self.connection_params['dbname']}"
            )
            return self._connection
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def disconnect(self):
        if self._connection:
            self._connection.close()
            print("Database connection closed")

    @property
    def connection(self):
        if not self._connection or self._connection.closed:
            self.connect()
        return self._connection

    def __repr__(self) -> str:
        return f"{self.connection_params['host']}:{self.connection_params['port']}@{self.connection_params['dbname']}"
