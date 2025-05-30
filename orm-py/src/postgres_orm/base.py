from db import DatabaseConnection
from typing import Dict, List, Any, Optional


class BaseModel:
    _db = None
    _shadows = []
    _table_name = None
    _primary_key = "id"

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def set_database(cls, db_connection: List[DatabaseConnection]):
        cls._db = db_connection[0]
        cls._shadows = db_connection[1:]

    @classmethod
    def create_table(cls, columns: Dict[str, str], if_not_exists: bool = True):
        """Create table with specified columns
        Args:
            columns: Dict of column_name: column_type
            if_not_exists: Add IF NOT EXISTS clause
        """
        if not cls._table_name:
            raise ValueError("Table name not specified")

        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""

        column_defs = []
        for col_name, col_type in columns.items():
            column_defs.append(f"{col_name} {col_type}")

        if cls._primary_key not in columns:
            column_defs.insert(0, f"{cls._primary_key} SERIAL PRIMARY KEY")

        columns_str = ", ".join(column_defs)

        query = f"""
        CREATE TABLE {if_not_exists_clause} {cls._table_name} (
            {columns_str}
        )
        """

        cls._execute_query(query, mirror=True)
        print(f"Table '{cls._table_name}' created successfully")

    @classmethod
    def drop_table(cls, if_exists: bool = True):
        if not cls._table_name:
            raise ValueError("Table name not specified")

        if_exists_clause = "IF EXISTS" if if_exists else ""
        query = f"DROP TABLE {if_exists_clause} {cls._table_name}"

        cls._execute_query(query, mirror=True)
        print(f"Table '{cls._table_name}' dropped successfully")

    @classmethod
    def _execute_query(cls, query: str, params: tuple = (), fetch: bool = False, mirror: bool = False, db: DatabaseConnection | None = None):
        if db is None:
            db = cls._db
        if not db:
            raise ValueError("Database connection not set. Use set_database() first.")

        conn = db.connection
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)

                if mirror:
                    print("got shadowing")
                    if len(cls._shadows) == 0:
                        raise ValueError("Please initialize BaseModel with more than one db to enable mirroring")
                    for shadow_db in cls._shadows:
                        print(f"running {query} on {shadow_db} ")
                        cls._execute_query(query, params, mirror=False, db=shadow_db)

                if fetch:
                    if (
                        query.strip().upper().startswith("SELECT")
                        or "RETURNING" in query.upper()
                    ):
                        return cursor.fetchall()
                    else:
                        return cursor.fetchone() if cursor.rowcount > 0 else None

                conn.commit()

                return cursor.rowcount
        except Exception as e:
            conn.rollback()
            print(f"Query execution error: {e}")
            print(f"Query: {query}")
            print(f"Params: {params}")
            raise

    @classmethod
    def create(cls, **data) -> "BaseModel":
        if not cls._table_name:
            raise ValueError("Table name not specified")

        filtered_data = {
            k: v for k, v in data.items() if v is not None and k != cls._primary_key
        }

        if not filtered_data:
            raise ValueError("No data provided for creation")

        columns = list(filtered_data.keys())
        placeholders = ["%s"] * len(columns)
        values = list(filtered_data.values())

        query = f"""
        INSERT INTO {cls._table_name} ({", ".join(columns)})
        VALUES ({", ".join(placeholders)})
        RETURNING *
        """

        cls._execute_query(query, tuple(values), fetch=True, mirror=True)

        raise Exception("Failed to create record")

    @classmethod
    def find_by_id(cls, record_id: Any) -> Optional["BaseModel"]:
        if not cls._table_name:
            raise ValueError("Table name not specified")

        query = f"SELECT * FROM {cls._table_name} WHERE {cls._primary_key} = %s"
        cls._execute_query(query, (record_id,), fetch=True)

        return None

    @classmethod
    def find_all(cls, where: str = None, params: tuple = None) -> List["BaseModel"]:
        if not cls._table_name:
            raise ValueError("Table name not specified")

        query = f"SELECT * FROM {cls._table_name}"
        if where:
            query += f" WHERE {where}"

        conn = cls._db.connection
        results = []

        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]

                for row in rows:
                    instance_data = dict(zip(column_names, row))
                    results.append(cls(**instance_data))
        except Exception as e:
            print(f"Query execution error: {e}")
            raise

        return results

    def save(self) -> "BaseModel":
        if not self._table_name:
            raise ValueError("Table name not specified")

        pk_value = getattr(self, self._primary_key, None)

        if pk_value and self.find_by_id(pk_value):
            return self.update()
        else:
            data = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
            new_instance = self.__class__.create(**data)

            for key, value in new_instance.__dict__.items():
                setattr(self, key, value)

            return self

    def update(self, **data) -> "BaseModel":
        if not self._table_name:
            raise ValueError("Table name not specified")

        pk_value = getattr(self, self._primary_key, None)
        if not pk_value:
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

        columns = list(update_data.keys())
        placeholders = [f"{col} = %s" for col in columns]
        values = list(update_data.values())
        values.append(pk_value)

        query = f"""
        UPDATE {self._table_name}
        SET {", ".join(placeholders)}
        WHERE {self._primary_key} = %s
        RETURNING *
        """

        self._execute_query(query, tuple(values), fetch=True, mirror=True)

        return self

    def delete(self) -> bool:
        if not self._table_name:
            raise ValueError("Table name not specified")

        pk_value = getattr(self, self._primary_key, None)
        if not pk_value:
            raise ValueError(f"No {self._primary_key} value found for deletion")

        query = f"DELETE FROM {self._table_name} WHERE {self._primary_key} = %s"
        rows_affected = self._execute_query(query, (pk_value,), mirror=True)

        return rows_affected > 0

    @classmethod
    def delete_by_id(cls, record_id: Any) -> bool:
        if not cls._table_name:
            raise ValueError("Table name not specified")

        query = f"DELETE FROM {cls._table_name} WHERE {cls._primary_key} = %s"
        rows_affected = cls._execute_query(query, (record_id,), mirror=True)

        return rows_affected > 0

    def __repr__(self):
        attrs = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return f"{self.__class__.__name__}({attrs})"
