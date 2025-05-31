from typing import Dict, List, Any
from ..base.declare import BaseDC, DatabaseType
from ..base.ops import BaseOperations


class MySQLOperations(BaseOperations):
    """MySQL-specific database operations"""

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
            column_defs.insert(0, f"{primary_key} INT AUTO_INCREMENT PRIMARY KEY")

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
        """

    def update_sql(self, table_name: str, columns: List[str], primary_key: str) -> str:
        placeholders = [f"{col} = %s" for col in columns]
        return f"""
        UPDATE {table_name}
        SET {", ".join(placeholders)}
        WHERE {primary_key} = %s
        """

    def execute_query(
        self,
        db: BaseDC,
        query: str,
        params: tuple = (),
        fetch: bool = False,
    ) -> Any:
        if db.db_type != DatabaseType.MYSQL:
            raise ValueError(f"Expected MySQL connection, got {db.db_type}")

        conn = db.connection
        cursor = conn.cursor()

        try:
            cursor.execute(query, params)

            if fetch:
                if query.strip().upper().startswith("SELECT"):
                    result = cursor.fetchall()
                    lastrowid = cursor.lastrowid
                    rowsAff = cursor.rowcount
                    cursor.close()
                    return {
                        "result": result,
                        "lastrowid": lastrowid,
                        "rowcount": rowsAff,
                    }
                else:
                    result = cursor.fetchone() if cursor.rowcount > 0 else None
                    lastrowid = cursor.lastrowid
                    rowsAff = cursor.rowcount
                    cursor.close()
                    return {
                        "result": result,
                        "lastrowid": lastrowid,
                        "rowcount": rowsAff,
                    }

            conn.commit()
            rowcount = cursor.rowcount
            last_id = cursor.lastrowid
            cursor.close()

            # Return both rowcount and last_id for insert operations
            return {"rowcount": rowcount, "lastrowid": last_id}

        except Exception as e:
            conn.rollback()
            if cursor:
                cursor.close()
            raise Exception from e

    def get_column_names(self, db: BaseDC, table_name: str) -> List[str]:
        cursor = db.connection.cursor()
        try:
            cursor.execute(f"DESCRIBE {table_name}")
            column_info = cursor.fetchall()
            column_names = [col[0] for col in column_info]
            cursor.close()
            return column_names
        except Exception as e:
            if cursor:
                cursor.close()
            raise e

    def handle_insert_result(
        self,
        db: BaseDC,
        table_name: str,
        primary_key: str,
        insert_result: Any,
        insert_params: tuple,
    ) -> Dict[str, Any]:
        """For MySQL, we need to fetch the created record using the lastrowid"""
        last_id = insert_result.get("lastrowid")
        if not last_id:
            raise Exception("Failed to get inserted record ID")

        # Fetch the created record
        select_query = f"SELECT * FROM {table_name} WHERE {primary_key} = %s"
        result = self.execute_query(db, select_query, (last_id,), fetch=True)

        if not result:
            raise Exception(f"Failed to retrieve created record with ID {last_id}")

        # Get column names and create instance data
        column_names = self.get_column_names(db, table_name)
        return dict(zip(column_names, result["result"][0]))

    def handle_update_result(
        self,
        db: BaseDC,
        table_name: str,
        primary_key: str,
        pk_value: Any,
        update_result: Any,
    ) -> Dict[str, Any]:
        """For MySQL, we need to fetch the updated record"""
        # Fetch the updated record
        select_query = f"SELECT * FROM {table_name} WHERE {primary_key} = %s"
        result = self.execute_query(db, select_query, (pk_value,), fetch=True)

        if not result:
            raise Exception(f"Failed to retrieve updated record with ID {pk_value}")

        # Get column names and create instance data
        column_names = self.get_column_names(db, table_name)
        return dict(zip(column_names, result["result"][0]))
