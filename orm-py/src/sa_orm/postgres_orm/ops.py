from typing import Dict, List, Any
from ..base.ops import BaseOperations
from ..base.declare import BaseDC, DatabaseType


class PostgreSQLOperations(BaseOperations):
    def create_table_sql(
        self,
        table_name: str,
        columns: Dict[str, str],
        primary_key: str,
        if_not_exists: bool = True,
    ) -> str:
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""

        column_defs = []
        for col_name, col_type in columns.items():
            column_defs.append(f"{col_name} {col_type}")

        # Add primary key if not specified in columns
        if primary_key not in columns:
            column_defs.insert(0, f"{primary_key} SERIAL PRIMARY KEY")

        columns_str = ", ".join(column_defs)

        return f"""
        CREATE TABLE {if_not_exists_clause} {table_name} (
            {columns_str}
        )
        """

    def insert_sql(self, table_name: str, columns: List[str]) -> str:
        placeholders = ["%s"] * len(columns)
        return f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({", ".join(placeholders)})
        RETURNING *
        """

    def update_sql(self, table_name: str, columns: List[str], primary_key: str) -> str:
        placeholders = [f"{col} = %s" for col in columns]
        return f"""
        UPDATE {table_name}
        SET {", ".join(placeholders)}
        WHERE {primary_key} = %s
        RETURNING *
        """

    def execute_query(
        self, db: BaseDC, query: str, params: tuple = (), fetch: bool = False
    ) -> Any:
        if db.db_type != DatabaseType.POSTGRESQL:
            raise ValueError(f"Expected PostgreSQL connection, got {db.db_type}")

        conn = db.connection

        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)

                if fetch:
                    if (
                        query.strip().upper().startswith("SELECT")
                        or "RETURNING" in query.upper()
                    ):
                        result = cursor.fetchall()
                        lastrowid = (
                            result[-1][0]
                            if result is not None and len(result) > 0
                            else None
                        )
                        rowsAff = cursor.rowcount
                        return {
                            "result": result,
                            "lastrowid": lastrowid,
                            "rowcount": rowsAff,
                        }
                    else:
                        result = cursor.fetchone() if cursor.rowcount > 0 else None
                        lastrowid = result[0] if result is not None else None
                        rowsAff = cursor.rowcount
                        return {
                            "result": result,
                            "lastrowid": lastrowid,
                            "rowcount": rowsAff,
                        }

                conn.commit()
                rowcount = cursor.rowcount
                return {"rowcount": rowcount}

        except Exception as e:
            conn.rollback()
            raise Exception from e

    def get_column_names(self, db: BaseDC, table_name: str) -> List[str]:
        with db.connection.cursor() as cursor:
            # Use a simple SELECT to get column names from cursor description
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            return [desc[0] for desc in cursor.description]

    def handle_insert_result(
        self,
        db: BaseDC,
        table_name: str,
        primary_key: str,
        insert_result: Any,
        insert_params: tuple,
    ) -> Dict[str, Any]:
        """For PostgreSQL, the RETURNING clause gives us the created record directly"""
        if not insert_result["result"]:
            raise Exception("Failed to create record")

        # Get column names
        column_names = self.get_column_names(db, table_name)
        return dict(zip(column_names, insert_result["result"][0]))

    def handle_update_result(
        self,
        db: BaseDC,
        table_name: str,
        primary_key: str,
        pk_value: Any,
        update_result: Any,
    ) -> Dict[str, Any]:
        """For PostgreSQL, the RETURNING clause gives us the updated record directly"""
        if not update_result["result"]:
            raise Exception(f"Failed to update record with ID {pk_value}")

        # Get column names
        column_names = self.get_column_names(db, table_name)
        return dict(zip(column_names, update_result["result"][0]))
