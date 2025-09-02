"""
Database Connector Module
Handles PostgreSQL database connections and basic operations.
"""

import psycopg2
import psycopg2.extras
import logging
from typing import Dict, List, Any, Optional
from contextlib import contextmanager


class DatabaseConnector:
    """PostgreSQL database connector with connection pooling and error handling."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize database connector with configuration.
        
        Args:
            config: Dictionary containing database connection parameters
                   (host, port, database, user, password)
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._connection = None
        
    def connect(self) -> psycopg2.extensions.connection:
        """Establish database connection."""
        # Try multiple connection strategies to handle encoding issues
        connection_strategies = [
            # Strategy 1: Default connection
            lambda: psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5432),
                database=self.config.get('database'),
                user=self.config.get('user'),
                password=self.config.get('password')
            ),
            # Strategy 2: Latin1 encoding
            lambda: psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5432),
                database=self.config.get('database'),
                user=self.config.get('user'),
                password=self.config.get('password'),
                client_encoding='latin1'
            ),
            # Strategy 3: Connection string with encoding
            lambda: psycopg2.connect(
                f"postgresql://{self.config.get('user')}:{self.config.get('password')}@{self.config.get('host', 'localhost')}:{self.config.get('port', 5432)}/{self.config.get('database')}?client_encoding=latin1"
            ),
            # Strategy 4: Minimal connection
            lambda: psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5432),
                database=self.config.get('database'),
                user=self.config.get('user'),
                password=self.config.get('password'),
                options='-c client_encoding=latin1'
            )
        ]
        
        last_error = None
        for i, strategy in enumerate(connection_strategies, 1):
            try:
                self._connection = strategy()
                self.logger.info(f"Connected to PostgreSQL database: {self.config.get('database')} (strategy {i})")
                return self._connection
            except Exception as e:
                last_error = e
                self.logger.warning(f"Connection strategy {i} failed: {e}")
                continue
        
        # If all strategies fail, raise the last error
        self.logger.error(f"All connection strategies failed. Last error: {last_error}")
        raise last_error
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """Context manager for database cursors."""
        if not self._connection or self._connection.closed:
            self.connect()
        
        cursor = self._connection.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            self.logger.error(f"Database operation failed: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
            
        Returns:
            List of tuples containing query results
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Handle encoding issues in results
            processed_results = []
            for row in results:
                processed_row = []
                for value in row:
                    if isinstance(value, bytes):
                        try:
                            processed_row.append(value.decode('latin1', errors='ignore'))
                        except Exception:
                            processed_row.append(str(value, errors='ignore'))
                    else:
                        processed_row.append(value)
                processed_results.append(tuple(processed_row))
            
            return processed_results
    
    def execute_query_dict(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results as dictionaries.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
            
        Returns:
            List of dictionaries containing query results
        """
        with self.get_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def execute_command(self, command: str, params: Optional[tuple] = None) -> int:
        """
        Execute a non-SELECT command (INSERT, UPDATE, DELETE).
        
        Args:
            command: SQL command string
            params: Command parameters (optional)
            
        Returns:
            Number of affected rows
        """
        with self.get_cursor() as cursor:
            cursor.execute(command, params)
            return cursor.rowcount
    
    def get_table_info(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get information about tables in the specified schema.
        
        Args:
            schema: Database schema name (default: 'public')
            
        Returns:
            List of dictionaries containing table information
        """
        query = """
        SELECT 
            table_name,
            (SELECT COUNT(*) FROM information_schema.columns 
             WHERE table_schema = %s AND table_name = t.table_name) as column_count
        FROM information_schema.tables t
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        return self.execute_query_dict(query, (schema, schema))
    
    def get_column_info(self, table_name: str, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get information about columns in a specific table.
        
        Args:
            table_name: Name of the table
            schema: Database schema name (default: 'public')
            
        Returns:
            List of dictionaries containing column information
        """
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        return self.execute_query_dict(query, (schema, table_name))
    
    def get_table_row_count(self, table_name: str, schema: str = 'public') -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            schema: Database schema name (default: 'public')
            
        Returns:
            Number of rows in the table
        """
        query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        result = self.execute_query(query)
        return result[0][0] if result else 0
    
    def get_schemas(self) -> List[str]:
        """
        Get list of available schemas in the database.
        
        Returns:
            List of schema names
        """
        query = """
        SELECT DISTINCT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
        """
        try:
            result = self.execute_query(query)
            schemas = []
            for row in result:
                try:
                    # Handle potential encoding issues
                    if isinstance(row[0], bytes):
                        schemas.append(row[0].decode('latin1', errors='ignore'))
                    else:
                        schemas.append(str(row[0]))
                except (UnicodeDecodeError, UnicodeEncodeError) as e:
                    self.logger.warning(f"Encoding issue with schema name: {e}")
                    # Try to recover by using a safe fallback
                    schemas.append(f"schema_{len(schemas)}")
            return schemas
        except Exception as e:
            self.logger.error(f"Error getting schemas: {e}")
            # Return default schemas if we can't get them from database
            return ['public']
    
    def get_schemas_safe(self, config_schemas: List[str] = None) -> List[str]:
        """
        Get schemas with fallback to default values if connection fails.
        
        Args:
            config_schemas: List of schemas from configuration file
            
        Returns:
            List of schema names
        """
        try:
            db_schemas = self.get_schemas()
            # Combine database schemas with config schemas
            all_schemas = list(set(db_schemas + (config_schemas or [])))
            all_schemas.sort()  # Sort alphabetically
            return all_schemas
        except Exception as e:
            self.logger.warning(f"Could not retrieve schemas from database: {e}")
            # Use config schemas if available, otherwise default to public
            if config_schemas:
                self.logger.info(f"Using schemas from configuration: {config_schemas}")
                return config_schemas
            else:
                self.logger.info("Using default schema: public")
                return ['public']
    
    def get_tables_by_schema(self, schema: str) -> List[str]:
        """
        Get list of tables in a specific schema.
        
        Args:
            schema: Database schema name
            
        Returns:
            List of table names in the schema
        """
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        try:
            result = self.execute_query(query, (schema,))
            tables = []
            for row in result:
                try:
                    # Handle potential encoding issues
                    if isinstance(row[0], bytes):
                        tables.append(row[0].decode('latin1', errors='ignore'))
                    else:
                        tables.append(str(row[0]))
                except (UnicodeDecodeError, UnicodeEncodeError) as e:
                    self.logger.warning(f"Encoding issue with table name: {e}")
                    # Try to recover by using a safe fallback
                    tables.append(f"table_{len(tables)}")
            return tables
        except Exception as e:
            self.logger.error(f"Error getting tables for schema {schema}: {e}")
            return []
    
    def get_tables_by_schema_safe(self, schema: str, config_tables: List[str] = None) -> List[str]:
        """
        Get tables in a schema with fallback to configuration or default values.
        
        Args:
            schema: Database schema name
            config_tables: List of tables from configuration file
            
        Returns:
            List of table names in the schema
        """
        try:
            db_tables = self.get_tables_by_schema(schema)
            if db_tables:
                return db_tables
        except Exception as e:
            self.logger.warning(f"Could not retrieve tables from database for schema {schema}: {e}")
        
        # If no tables found in database, use config tables or provide helpful message
        if config_tables:
            self.logger.info(f"Using tables from configuration for schema {schema}: {config_tables}")
            return config_tables
        else:
            self.logger.info(f"No tables found in schema '{schema}' and no configuration provided")
            self.logger.info("You can add tables to the configuration file or create tables in the database")
            return []
    
    def close(self):
        """Close the database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self.logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
