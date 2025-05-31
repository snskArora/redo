from base.declare import BaseDC, DatabaseType
from typing import Dict, List, Any, Optional

from base.ops import BaseOperations
from mysql_orm.ops import MySQLOperations
from postgres_orm.ops import PostgreSQLOperations
from log import Logger

Log = Logger()
log = Log.log
log_op = Log.log_op


class OperationsFactory:
    """Factory to create appropriate database operations instance"""

    _operations_cache = {}

    @classmethod
    def get_operations(cls, db: BaseDC) -> BaseOperations:
        """Get the appropriate operations instance for the database type"""
        if db.db_type not in cls._operations_cache:
            match db.db_type:
                case DatabaseType.MYSQL:
                    cls._operations_cache[db.db_type] = MySQLOperations()
                case DatabaseType.POSTGRESQL:
                    cls._operations_cache[db.db_type] = PostgreSQLOperations()

        return cls._operations_cache[db.db_type]


class BaseModel:
    _db = None
    _shadows = []
    _table_name = None
    _primary_key = "id"

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def set_database(cls, db_connections: List[BaseDC]):
        if not db_connections:
            raise ValueError("At least one database connection required")

        cls._db = db_connections[0]
        cls._shadows = db_connections[1:]

    @classmethod
    def disconnect(cls):
        if cls._db is None:
            raise ValueError("No active database connection found")
        cls._db.disconnect()
        for d in cls._shadows:
            d.disconnect()

    # @classmethod
    # def add_shadow(cls, db_connection: BaseDC):
    #     if not db_connection:
    #         raise ValueError("At least one database connection required")
    #
    #     primary_ops = OperationsFactory.get_operations(db_connection)
    #     query = primary_ops.create_table_sql(
    #         cls._table_name, columns, cls._primary_key, True
    #     )
    #     primary_ops.execute_query(cls._db, query)
    #     cls._shadows.append(db_connection)

    @classmethod
    def create_table(cls, columns: Dict[str, str], if_not_exists: bool = True):
        if not cls._table_name:
            log_op(
                action="create_table",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={"payload": "ValueError: Table name not specified"},
            )
            raise ValueError("Table name not specified")

        if not cls._db:
            raise ValueError("Database connection not set. Use set_database() first.")

        primary_ops = OperationsFactory.get_operations(cls._db)
        query = primary_ops.create_table_sql(
            cls._table_name, columns, cls._primary_key, if_not_exists
        )
        primary_ops.execute_query(cls._db, query)

        if cls._shadows:
            for shadow_db in cls._shadows:
                shadow_ops = OperationsFactory.get_operations(shadow_db)
                shadow_query = shadow_ops.create_table_sql(
                    cls._table_name, columns, cls._primary_key, if_not_exists
                )
                shadow_ops.execute_query(shadow_db, shadow_query)

        log(f"Table '{cls._table_name}' created successfully on all databases", "INFO")
        log_op(
            action="create_table",
            table=f"{cls._db}:{cls._table_name}",
            metadata={"payload": f"table {cls._table_name} created"},
        )

    @classmethod
    def drop_table(cls, if_exists: bool = True):
        """Drop table from all databases"""
        if not cls._table_name:
            log_op(
                action="drop_table",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={"payload": "ValueError: Table name not specified"},
            )
            raise ValueError("Table name not specified")

        if not cls._db:
            log_op(
                action="drop_table",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={
                    "payload": "Database connection not set. Use set_database() first."
                },
            )
            raise ValueError("Database connection not set. Use set_database() first.")

        if_exists_clause = "IF EXISTS" if if_exists else ""
        query = f"DROP TABLE {if_exists_clause} {cls._table_name}"

        # Drop from primary database
        primary_ops = OperationsFactory.get_operations(cls._db)
        primary_ops.execute_query(cls._db, query)

        # Mirror to shadow databases
        if cls._shadows:
            for shadow_db in cls._shadows:
                shadow_ops = OperationsFactory.get_operations(shadow_db)
                shadow_ops.execute_query(shadow_db, query)

        log(
            f"Table '{cls._table_name}' dropped successfully from all databases", "INFO"
        )
        log_op(
            action="update",
            table=f"{self._db}:{self._table_name}",
            record_id=getattr(self, self._primary_key, None),
            success=True,
            metadata={"payload": f"Record updated: {instance_data}"},
        )

    @classmethod
    def _mirror_operation(cls, operation_func, *args, **kwargs):
        """Execute operation on primary db and mirror to shadows"""
        primary_result = operation_func(cls._db, *args, **kwargs)

        if cls._shadows:
            shadow_failed = []
            for shadow_db in cls._shadows:
                try:
                    operation_func(shadow_db, *args, **kwargs)
                except Exception as e:
                    shadow_failed.append({f"{shadow_db}": e})
                    log(f"Failed to mirror operation to {shadow_db}: {e}", "ERROR")
            if len(shadow_failed) > 0:
                raise Exception(
                    f"Update failed on the following shadows: {shadow_failed}"
                )

        return primary_result

    @classmethod
    def create(cls, **data) -> "BaseModel":
        """Create a new record in all databases"""
        if not cls._table_name:
            log_op(
                action="create",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={"payload": "Table name not specified"},
            )
            raise ValueError("Table name not specified")

        if not cls._db:
            log_op(
                action="create",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={
                    "payload": "Database connection not set. Use set_database() first."
                },
            )
            raise ValueError("Database connection not set. Use set_database() first.")

        columns, values = [], []
        for k, v in data.items():
            if v is not None and k != cls._primary_key:
                columns.append(k)
                values.append(v)

        if len(columns) == 0:
            log_op(
                action="create",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={"payload": "No data provided for creation"},
            )
            raise ValueError("No data provided for creation")

        ops = OperationsFactory.get_operations(cls._db)

        query = ops.insert_sql(cls._table_name, columns)
        result = ops.execute_query(cls._db, query, tuple(values), fetch=True)
        instance_data = ops.handle_insert_result(
            cls._db, cls._table_name, cls._primary_key, result, tuple(values)
        )

        columns.append(cls._primary_key)
        values.append(result["lastrowid"])

        failed_shadows = []
        for shadow_db in cls._shadows:
            try:
                ops = OperationsFactory.get_operations(shadow_db)
                ops.execute_query(
                    shadow_db, ops.insert_sql(cls._table_name, columns), tuple(values)
                )
            except Exception as e:
                failed_shadows.append({f"{shadow_db}": e})

        if len(failed_shadows) > 0:
            log_op(
                action="create",
                table=f"{cls._db}:{cls._table_name}",
                record_id=result["lastrowid"],
                success=False,
                metadata={"payload": f"Failed to create shadows: {failed_shadows}"},
            )
            raise Exception(f"Failed to create shadows: {failed_shadows}")

        return cls(**instance_data)

    @classmethod
    def find_by_id(cls, record_id: Any) -> Optional["BaseModel"]:
        """Find record by ID (reads from primary database only)"""
        if not cls._table_name:
            log_op(
                action="find_by_id",
                table=f"{cls._db}:{cls._table_name}",
                record_id=record_id,
                success=False,
                metadata={"payload": "Table name not specified"},
            )
            raise ValueError("Table name not specified")

        if not cls._db:
            log_op(
                action="find_by_id",
                table=f"{cls._db}:{cls._table_name}",
                record_id=record_id,
                success=False,
                metadata={
                    "payload": "Database connection not set. Use set_database() first."
                },
            )
            raise ValueError("Database connection not set. Use set_database() first.")

        primary_ops = OperationsFactory.get_operations(cls._db)
        query = f"SELECT * FROM {cls._table_name} WHERE {cls._primary_key} = %s"
        result = primary_ops.execute_query(cls._db, query, (record_id,), fetch=True)

        if result["result"]:
            column_names = primary_ops.get_column_names(cls._db, cls._table_name)
            instance_data = dict(zip(column_names, result["result"][0]))
            return cls(**instance_data)

        return None

    @classmethod
    def find_all(cls, where: str = None, params: tuple = None) -> List["BaseModel"]:
        """Find all records matching criteria (reads from primary database only)"""
        if not cls._table_name:
            log_op(
                action="find_all",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={"payload": "Table name not specified"},
            )
            raise ValueError("Table name not specified")

        if not cls._db:
            log_op(
                action="find_all",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={
                    "payload": "Database connection not set. Use set_database() first."
                },
            )
            raise ValueError("Database connection not set. Use set_database() first.")

        query = f"SELECT * FROM {cls._table_name}"
        if where:
            query += f" WHERE {where}"

        primary_ops = OperationsFactory.get_operations(cls._db)
        results = []

        try:
            rows = primary_ops.execute_query(cls._db, query, params, fetch=True)
            column_names = primary_ops.get_column_names(cls._db, cls._table_name)

            for row in rows:
                instance_data = dict(zip(column_names, row))
                results.append(cls(**instance_data))

        except Exception as e:
            log_op(
                action="find_all",
                table=f"{cls._db}:{cls._table_name}",
                success=False,
                metadata={"payload": f"Query execution error: {e}"},
            )
            log(f"Query execution error: {e}", "ERROR")
            raise

        return results

    def save(self) -> "BaseModel":
        """Save the current instance (create or update)"""
        if not self._table_name:
            log_op(
                action="save",
                table=f"{self._db}:{self._table_name}",
                record_id=getattr(self, self._primary_key, None),
                success=False,
                metadata={"payload": "Table name not specified"},
            )
            raise ValueError("Table name not specified")

        pk_value = getattr(self, self._primary_key, None)

        if pk_value and self.find_by_id(pk_value):
            return self.update()
        else:
            data = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
            new_instance = self.__class__.update(self, **data)

            for key, value in new_instance.__dict__.items():
                setattr(self, key, value)

            return self

    def update(self, **data) -> "BaseModel":
        """Update the current record in all databases"""
        if not self._table_name:
            log_op(
                action="update",
                table=f"{self._db}:{self._table_name}",
                record_id=getattr(self, self._primary_key, None),
                success=False,
                metadata={"payload": "Table name not specified"},
            )
            raise ValueError("Table name not specified")

        pk_value = getattr(self, self._primary_key, None)
        if not pk_value:
            log_op(
                action="update",
                table=f"{self._db}:{self._table_name}",
                record_id=pk_value,
                success=False,
                metadata={"payload": f"No {self._primary_key} value found for update"},
            )
            raise ValueError(f"No {self._primary_key} value found for update")

        update_data = (
            data
            if data
            else {
                k: v
                for k, v in self.__dict__.items()
                if not k.startswith("_") and k != self._primary_key
            }
        )

        if not update_data:
            return self

        def update_operation(db: BaseDC):
            ops = OperationsFactory.get_operations(db)
            columns = list(update_data.keys())
            values = list(update_data.values())
            values.append(pk_value)

            query = ops.update_sql(self._table_name, columns, self._primary_key)
            result = ops.execute_query(db, query, tuple(values), fetch=True)

            return ops.handle_update_result(
                db, self._table_name, self._primary_key, pk_value, result
            )

        # Execute update operation with mirroring
        instance_data = self._mirror_operation(update_operation)

        # Update current instance with new data
        for key, value in instance_data.items():
            setattr(self, key, value)

        log_op(
            action="update",
            table=f"{self._db}:{self._table_name}",
            record_id=pk_value,
            success=True,
            metadata={"payload": f"Record updated: {instance_data}"},
        )
        return self

    def delete(self) -> bool:
        """Delete the current record from all databases"""
        if not self._table_name:
            log_op(
                action="delete",
                table=f"{self._db}:{self._table_name}",
                record_id=getattr(self, self._primary_key, None),
                success=False,
                metadata={"payload": "Table name not specified"},
            )
            raise ValueError("Table name not specified")

        pk_value = getattr(self, self._primary_key, None)
        if not pk_value:
            log_op(
                action="delete",
                table=f"{self._db}:{self._table_name}",
                record_id=pk_value,
                success=False,
                metadata={
                    "payload": f"No {self._primary_key} value found for deletion"
                },
            )
            raise ValueError(f"No {self._primary_key} value found for deletion")

        def delete_operation(db: BaseDC):
            ops = OperationsFactory.get_operations(db)
            query = f"DELETE FROM {self._table_name} WHERE {self._primary_key} = %s"
            return ops.execute_query(db, query, (pk_value,))

        # Execute delete operation with mirroring
        rows_affected = self._mirror_operation(delete_operation)["rowcount"]
        return rows_affected > 0

    @classmethod
    def delete_by_id(cls, record_id: Any) -> bool:
        """Delete record by ID from all databases"""
        if not cls._table_name:
            log_op(
                action="delete",
                table=f"{cls._db}:{cls._table_name}",
                record_id=getattr(cls, cls._primary_key, None),
                success=False,
                metadata={"payload": "Table name not specified"},
            )
            raise ValueError("Table name not specified")

        def delete_operation(db: BaseDC):
            ops = OperationsFactory.get_operations(db)
            query = f"DELETE FROM {cls._table_name} WHERE {cls._primary_key} = %s"
            return ops.execute_query(db, query, (record_id,))

        # Execute delete operation with mirroring
        rows_affected = cls._mirror_operation(delete_operation)
        log_op(
            action="delete_by_id",
            table=f"{cls._db}:{cls._table_name}",
            record_id=record_id,
            success=rows_affected > 0,
            metadata={
                "payload": "Record deleted" if rows_affected > 0 else "No rows deleted"
            },
        )
        return rows_affected > 0

    def __repr__(self):
        attrs = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return f"{self.__class__.__name__}({attrs})"
