from abc import ABC, abstractmethod
from typing import Dict, List, Any
from base.declare import BaseDC


class BaseOperations(ABC):
    """Abstract base class defining the interface for database operations"""

    @abstractmethod
    def create_table_sql(
        self,
        table_name: str,
        columns: Dict[str, str],
        primary_key: str,
        if_not_exists: bool = True,
    ) -> str:
        """Generate CREATE TABLE SQL for the specific database type"""
        pass

    @abstractmethod
    def insert_sql(self, table_name: str, columns: List[str]) -> str:
        """Generate INSERT SQL that returns the created record"""
        pass

    @abstractmethod
    def update_sql(self, table_name: str, columns: List[str], primary_key: str) -> str:
        """Generate UPDATE SQL that returns the updated record"""
        pass

    @abstractmethod
    def execute_query(
        self,
        db: BaseDC,
        query: str,
        params: tuple = (),
        fetch: bool = False,
    ) -> Any:
        """Execute query on the specific database type"""
        pass

    @abstractmethod
    def get_column_names(self, db: BaseDC, table_name: str) -> List[str]:
        """Get column names for a table"""
        pass

    @abstractmethod
    def handle_insert_result(
        self,
        db: BaseDC,
        table_name: str,
        primary_key: str,
        insert_result: Any,
        insert_params: tuple,
    ) -> Dict[str, Any]:
        """Handle the result of an insert operation to return the created record data"""
        pass

    @abstractmethod
    def handle_update_result(
        self,
        db: BaseDC,
        table_name: str,
        primary_key: str,
        pk_value: Any,
        update_result: Any,
    ) -> Dict[str, Any]:
        """Handle the result of an update operation to return the updated record data"""
        pass
