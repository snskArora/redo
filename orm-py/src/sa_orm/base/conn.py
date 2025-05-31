from .declare import DatabaseType
from ..mysql_orm.db import DatabaseConnection as mysqlDC
from ..postgres_orm.db import DatabaseConnection as postgresDC


def createConnection(
    host: str, port: int, database: str, user: str, password: str, db_type: DatabaseType
):
    match db_type:
        case DatabaseType.MYSQL:
            return mysqlDC(host, port, database, user, password)
        case DatabaseType.POSTGRESQL:
            return postgresDC(host, port, database, user, password)
