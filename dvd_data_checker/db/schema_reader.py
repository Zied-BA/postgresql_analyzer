"""
Schema Reader Module
Provides functionality to read and analyze database schemas and table structures.
"""

import logging
from typing import Dict, List, Any, Optional
from .connector import DatabaseConnector


class SchemaReader:
    """Reads and analyzes PostgreSQL database schemas and table structures."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initialize schema reader with database connector.
        
        Args:
            db_connector: DatabaseConnector instance
        """
        self.db = db_connector
        self.logger = logging.getLogger(__name__)
    
    def get_schemas(self) -> List[Dict[str, Any]]:
        """
        Get list of all schemas in the database.
        
        Returns:
            List of dictionaries containing schema information
        """
        query = """
        SELECT 
            schema_name,
            schema_owner,
            default_character_set_name,
            default_collation_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
        """
        return self.db.execute_query_dict(query)
    
    def get_tables_by_schema(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get all tables in a specific schema.
        
        Args:
            schema: Schema name (default: 'public')
            
        Returns:
            List of dictionaries containing table information
        """
        query = """
        SELECT 
            table_name,
            table_type,
            (SELECT COUNT(*) FROM information_schema.columns 
             WHERE table_schema = %s AND table_name = t.table_name) as column_count,
            (SELECT COUNT(*) FROM %s.%s WHERE 1=1) as row_count
        FROM information_schema.tables t
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        
        # Get tables first
        tables = self.db.execute_query_dict(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE'",
            (schema,)
        )
        
        result = []
        for table in tables:
            table_name = table['table_name']
            try:
                # Get row count for each table
                row_count_query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
                row_count = self.db.execute_query(row_count_query)[0][0]
                
                # Get column count
                column_count_query = """
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                """
                column_count = self.db.execute_query(column_count_query, (schema, table_name))[0][0]
                
                result.append({
                    'table_name': table_name,
                    'table_type': 'BASE TABLE',
                    'column_count': column_count,
                    'row_count': row_count
                })
            except Exception as e:
                self.logger.warning(f"Could not get info for table {schema}.{table_name}: {e}")
                result.append({
                    'table_name': table_name,
                    'table_type': 'BASE TABLE',
                    'column_count': 0,
                    'row_count': 0
                })
        
        return result
    
    def get_table_structure(self, table_name: str, schema: str = 'public') -> Dict[str, Any]:
        """
        Get detailed structure of a specific table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary containing table structure information
        """
        # Get basic table info
        table_info_query = """
        SELECT 
            table_name,
            table_type,
            (SELECT COUNT(*) FROM %s.%s) as row_count
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """
        
        table_info = self.db.execute_query_dict(table_info_query, (schema, table_name, schema, table_name))
        
        if not table_info:
            raise ValueError(f"Table {schema}.{table_name} not found")
        
        # Get column information
        columns_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        
        columns = self.db.execute_query_dict(columns_query, (schema, table_name))
        
        # Get primary key information
        pk_query = """
        SELECT 
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY' 
            AND tc.table_schema = %s 
            AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
        """
        
        primary_keys = [row['column_name'] for row in self.db.execute_query_dict(pk_query, (schema, table_name))]
        
        # Get foreign key information
        fk_query = """
        SELECT 
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu 
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_schema = %s 
            AND tc.table_name = %s
        """
        
        foreign_keys = self.db.execute_query_dict(fk_query, (schema, table_name))
        
        return {
            'table_name': table_name,
            'schema': schema,
            'table_type': table_info[0]['table_type'],
            'row_count': table_info[0]['row_count'],
            'columns': columns,
            'primary_keys': primary_keys,
            'foreign_keys': foreign_keys
        }
    
    def get_database_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the entire database structure.
        
        Returns:
            Dictionary containing database summary
        """
        schemas = self.get_schemas()
        summary = {
            'schemas': schemas,
            'total_schemas': len(schemas),
            'tables_by_schema': {},
            'total_tables': 0,
            'total_rows': 0
        }
        
        for schema in schemas:
            schema_name = schema['schema_name']
            tables = self.get_tables_by_schema(schema_name)
            summary['tables_by_schema'][schema_name] = tables
            summary['total_tables'] += len(tables)
            summary['total_rows'] += sum(table['row_count'] for table in tables)
        
        return summary
    
    def find_tables_by_pattern(self, pattern: str, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Find tables matching a specific pattern.
        
        Args:
            pattern: SQL LIKE pattern to match table names
            schema: Schema name (default: 'public')
            
        Returns:
            List of matching tables
        """
        query = """
        SELECT 
            table_name,
            (SELECT COUNT(*) FROM information_schema.columns 
             WHERE table_schema = %s AND table_name = t.table_name) as column_count
        FROM information_schema.tables t
        WHERE table_schema = %s AND table_name LIKE %s
        ORDER BY table_name
        """
        return self.db.execute_query_dict(query, (schema, schema, pattern))
    
    def get_date_columns(self, table_name: str, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get all date/timestamp columns in a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: 'public')
            
        Returns:
            List of date column information
        """
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s 
            AND table_name = %s 
            AND data_type IN ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone')
        ORDER BY ordinal_position
        """
        return self.db.execute_query_dict(query, (schema, table_name))
