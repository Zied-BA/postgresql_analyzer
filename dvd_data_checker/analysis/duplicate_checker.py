"""
Duplicate Checker Module
Detects duplicate records in database tables and provides analysis reports.
"""

import logging
from typing import Dict, List, Any, Optional
from db.connector import DatabaseConnector
from db.schema_reader import SchemaReader


class DuplicateChecker:
    """Detects and analyzes duplicate records in database tables."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initialize duplicate checker with database connector.
        
        Args:
            db_connector: DatabaseConnector instance
        """
        self.db = db_connector
        self.schema_reader = SchemaReader(db_connector)
        self.logger = logging.getLogger(__name__)
    
    def check_table_duplicates(self, table_name: str, schema: str = 'public', 
                             columns: List[str] = None) -> Dict[str, Any]:
        """
        Check for duplicate records in a specific table.
        
        Args:
            table_name: Name of the table to analyze
            schema: Schema name (default: 'public')
            columns: List of columns to check for duplicates (default: all columns)
            
        Returns:
            Dictionary containing duplicate analysis results
        """
        try:
            # Get table structure
            table_structure = self.schema_reader.get_table_structure(table_name, schema)
            
            if columns is None:
                # Use all columns for duplicate checking
                columns = [col['column_name'] for col in table_structure['columns']]
            
            # Build the duplicate detection query
            column_list = ', '.join(columns)
            group_by_clause = ', '.join(columns)
            
            duplicate_query = f"""
            SELECT {column_list}, COUNT(*) as duplicate_count
            FROM {schema}.{table_name}
            GROUP BY {group_by_clause}
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            """
            
            duplicates = self.db.execute_query_dict(duplicate_query)
            
            # Calculate total duplicate rows
            total_duplicate_rows = sum(row['duplicate_count'] for row in duplicates)
            total_duplicate_groups = len(duplicates)
            
            # Calculate percentage of duplicates
            total_rows = table_structure['row_count']
            duplicate_percentage = (total_duplicate_rows / total_rows) * 100 if total_rows > 0 else 0
            
            duplicate_analysis = {
                'table_name': table_name,
                'schema': schema,
                'columns_checked': columns,
                'total_rows': total_rows,
                'total_duplicate_groups': total_duplicate_groups,
                'total_duplicate_rows': total_duplicate_rows,
                'duplicate_percentage': round(duplicate_percentage, 2),
                'severity': self._calculate_severity(duplicate_percentage),
                'duplicate_details': duplicates
            }
            
            return duplicate_analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing duplicates in {schema}.{table_name}: {e}")
            return {
                'table_name': table_name,
                'schema': schema,
                'error': str(e),
                'severity': 'error'
            }
    
    def check_primary_key_duplicates(self, table_name: str, schema: str = 'public') -> Dict[str, Any]:
        """
        Check for primary key duplicates (which should not exist).
        
        Args:
            table_name: Name of the table to analyze
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary containing primary key duplicate analysis
        """
        try:
            table_structure = self.schema_reader.get_table_structure(table_name, schema)
            primary_keys = table_structure['primary_keys']
            
            if not primary_keys:
                return {
                    'table_name': table_name,
                    'schema': schema,
                    'primary_keys': [],
                    'has_primary_key': False,
                    'duplicate_count': 0,
                    'severity': 'info'
                }
            
            # Check for primary key duplicates
            pk_columns = ', '.join(primary_keys)
            pk_duplicate_query = f"""
            SELECT {pk_columns}, COUNT(*) as duplicate_count
            FROM {schema}.{table_name}
            GROUP BY {pk_columns}
            HAVING COUNT(*) > 1
            """
            
            pk_duplicates = self.db.execute_query_dict(pk_duplicate_query)
            
            return {
                'table_name': table_name,
                'schema': schema,
                'primary_keys': primary_keys,
                'has_primary_key': True,
                'duplicate_count': len(pk_duplicates),
                'duplicate_details': pk_duplicates,
                'severity': 'critical' if pk_duplicates else 'low'
            }
            
        except Exception as e:
            self.logger.error(f"Error checking primary key duplicates in {schema}.{table_name}: {e}")
            return {
                'table_name': table_name,
                'schema': schema,
                'error': str(e),
                'severity': 'error'
            }
    
    def check_business_key_duplicates(self, table_name: str, schema: str = 'public',
                                    business_keys: Dict[str, List[str]] = None) -> Dict[str, Any]:
        """
        Check for duplicates based on business keys (e.g., email, customer_id).
        
        Args:
            table_name: Name of the table to analyze
            schema: Schema name (default: 'public')
            business_keys: Dictionary mapping table names to business key columns
            
        Returns:
            Dictionary containing business key duplicate analysis
        """
        if business_keys is None:
            # Default business keys for common table patterns
            business_keys = {
                'customer': ['email', 'first_name', 'last_name'],
                'film': ['title', 'release_year'],
                'rental': ['customer_id', 'inventory_id', 'rental_date'],
                'payment': ['customer_id', 'rental_id', 'amount', 'payment_date']
            }
        
        table_business_keys = business_keys.get(table_name, [])
        
        if not table_business_keys:
            return {
                'table_name': table_name,
                'schema': schema,
                'business_keys_checked': [],
                'has_business_keys': False,
                'severity': 'info'
            }
        
        try:
            # Check for business key duplicates
            bk_columns = ', '.join(table_business_keys)
            bk_duplicate_query = f"""
            SELECT {bk_columns}, COUNT(*) as duplicate_count
            FROM {schema}.{table_name}
            GROUP BY {bk_columns}
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            """
            
            bk_duplicates = self.db.execute_query_dict(bk_duplicate_query)
            
            # Calculate severity based on business impact
            total_duplicates = sum(row['duplicate_count'] for row in bk_duplicates)
            total_rows = self.db.get_table_row_count(table_name, schema)
            duplicate_percentage = (total_duplicates / total_rows) * 100 if total_rows > 0 else 0
            
            return {
                'table_name': table_name,
                'schema': schema,
                'business_keys_checked': table_business_keys,
                'has_business_keys': True,
                'total_duplicate_groups': len(bk_duplicates),
                'total_duplicate_rows': total_duplicates,
                'duplicate_percentage': round(duplicate_percentage, 2),
                'duplicate_details': bk_duplicates,
                'severity': self._calculate_severity(duplicate_percentage)
            }
            
        except Exception as e:
            self.logger.error(f"Error checking business key duplicates in {schema}.{table_name}: {e}")
            return {
                'table_name': table_name,
                'schema': schema,
                'error': str(e),
                'severity': 'error'
            }
    
    def check_schema_duplicates(self, schema: str = 'public') -> Dict[str, Any]:
        """
        Check for duplicates across all tables in a schema.
        
        Args:
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary containing duplicate analysis for all tables
        """
        try:
            tables = self.schema_reader.get_tables_by_schema(schema)
            
            schema_analysis = {
                'schema': schema,
                'total_tables': len(tables),
                'tables_analyzed': 0,
                'tables_with_duplicates': 0,
                'critical_issues': 0,
                'high_issues': 0,
                'medium_issues': 0,
                'low_issues': 0,
                'table_results': []
            }
            
            for table in tables:
                table_name = table['table_name']
                self.logger.info(f"Analyzing duplicates in {schema}.{table_name}")
                
                # Check general duplicates
                general_duplicates = self.check_table_duplicates(table_name, schema)
                
                # Check primary key duplicates
                pk_duplicates = self.check_primary_key_duplicates(table_name, schema)
                
                # Check business key duplicates
                bk_duplicates = self.check_business_key_duplicates(table_name, schema)
                
                table_analysis = {
                    'table_name': table_name,
                    'general_duplicates': general_duplicates,
                    'primary_key_duplicates': pk_duplicates,
                    'business_key_duplicates': bk_duplicates
                }
                
                schema_analysis['table_results'].append(table_analysis)
                schema_analysis['tables_analyzed'] += 1
                
                # Count issues by severity
                severities = [
                    general_duplicates.get('severity', 'low'),
                    pk_duplicates.get('severity', 'low'),
                    bk_duplicates.get('severity', 'low')
                ]
                
                if 'error' not in severities:
                    if any(dup.get('total_duplicate_rows', 0) > 0 for dup in [general_duplicates, bk_duplicates]):
                        schema_analysis['tables_with_duplicates'] += 1
                    
                    if 'critical' in severities:
                        schema_analysis['critical_issues'] += 1
                    elif 'high' in severities:
                        schema_analysis['high_issues'] += 1
                    elif 'medium' in severities:
                        schema_analysis['medium_issues'] += 1
                    elif 'low' in severities:
                        schema_analysis['low_issues'] += 1
            
            return schema_analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing schema {schema}: {e}")
            return {
                'schema': schema,
                'error': str(e),
                'severity': 'error'
            }
    
    def run_analysis(self, schemas: List[str] = None) -> List[Dict[str, Any]]:
        """
        Run duplicate analysis on specified schemas.
        
        Args:
            schemas: List of schema names to analyze (default: all schemas)
            
        Returns:
            List of analysis results for each schema
        """
        if schemas is None:
            schemas = [schema['schema_name'] for schema in self.schema_reader.get_schemas()]
        
        results = []
        
        for schema in schemas:
            self.logger.info(f"Starting duplicate analysis for schema: {schema}")
            schema_result = self.check_schema_duplicates(schema)
            results.append(schema_result)
        
        return results
    
    def generate_duplicate_report(self, analysis_results: List[Dict[str, Any]]) -> str:
        """
        Generate a human-readable report from duplicate analysis results.
        
        Args:
            analysis_results: Results from duplicate analysis
            
        Returns:
            Formatted report string
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("DUPLICATE RECORD ANALYSIS REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        total_critical = 0
        total_high = 0
        total_medium = 0
        total_low = 0
        
        for schema_result in analysis_results:
            if 'error' in schema_result:
                report_lines.append(f"ERROR in schema {schema_result['schema']}: {schema_result['error']}")
                report_lines.append("")
                continue
            
            schema = schema_result['schema']
            report_lines.append(f"SCHEMA: {schema}")
            report_lines.append("-" * 40)
            report_lines.append(f"Tables analyzed: {schema_result['tables_analyzed']}")
            report_lines.append(f"Tables with duplicates: {schema_result['tables_with_duplicates']}")
            report_lines.append("")
            
            # Summary by severity
            if schema_result['critical_issues'] > 0:
                report_lines.append(f"  CRITICAL issues: {schema_result['critical_issues']}")
                total_critical += schema_result['critical_issues']
            if schema_result['high_issues'] > 0:
                report_lines.append(f"  HIGH issues: {schema_result['high_issues']}")
                total_high += schema_result['high_issues']
            if schema_result['medium_issues'] > 0:
                report_lines.append(f"  MEDIUM issues: {schema_result['medium_issues']}")
                total_medium += schema_result['medium_issues']
            if schema_result['low_issues'] > 0:
                report_lines.append(f"  LOW issues: {schema_result['low_issues']}")
                total_low += schema_result['low_issues']
            
            report_lines.append("")
            
            # Detailed table results
            for table_result in schema_result['table_results']:
                table_name = table_result['table_name']
                general_dup = table_result['general_duplicates']
                pk_dup = table_result['primary_key_duplicates']
                bk_dup = table_result['business_key_duplicates']
                
                has_issues = False
                
                # Check primary key duplicates (critical)
                if pk_dup.get('severity') == 'critical':
                    report_lines.append(f"  Table: {table_name} - CRITICAL: Primary key duplicates found!")
                    report_lines.append(f"    Primary keys: {', '.join(pk_dup.get('primary_keys', []))}")
                    report_lines.append(f"    Duplicate groups: {pk_dup.get('duplicate_count', 0)}")
                    has_issues = True
                
                # Check business key duplicates
                if bk_dup.get('total_duplicate_rows', 0) > 0:
                    report_lines.append(f"  Table: {table_name} - {bk_dup.get('severity', 'low').upper()}: Business key duplicates")
                    report_lines.append(f"    Business keys: {', '.join(bk_dup.get('business_keys_checked', []))}")
                    report_lines.append(f"    Duplicate rows: {bk_dup.get('total_duplicate_rows', 0)} ({bk_dup.get('duplicate_percentage', 0)}%)")
                    has_issues = True
                
                # Check general duplicates
                if general_dup.get('total_duplicate_rows', 0) > 0:
                    report_lines.append(f"  Table: {table_name} - {general_dup.get('severity', 'low').upper()}: General duplicates")
                    report_lines.append(f"    Duplicate rows: {general_dup.get('total_duplicate_rows', 0)} ({general_dup.get('duplicate_percentage', 0)}%)")
                    has_issues = True
                
                if has_issues:
                    report_lines.append("")
            
            report_lines.append("")
        
        # Overall summary
        report_lines.append("=" * 80)
        report_lines.append("OVERALL SUMMARY")
        report_lines.append("=" * 80)
        report_lines.append(f"Critical issues: {total_critical}")
        report_lines.append(f"High issues: {total_high}")
        report_lines.append(f"Medium issues: {total_medium}")
        report_lines.append(f"Low issues: {total_low}")
        
        if total_critical > 0:
            report_lines.append("\n🚨 CRITICAL: Primary key duplicates found - immediate action required!")
        elif total_high > 0:
            report_lines.append("\n⚠️  HIGH: Significant duplicate data detected")
        elif total_medium > 0:
            report_lines.append("\n⚠️  MEDIUM: Some duplicate data found")
        else:
            report_lines.append("\n✅ No significant duplicate issues found")
        
        return "\n".join(report_lines)
    
    def _calculate_severity(self, duplicate_percentage: float) -> str:
        """
        Calculate severity level based on duplicate percentage.
        
        Args:
            duplicate_percentage: Percentage of duplicate rows
            
        Returns:
            Severity level (critical, high, medium, low)
        """
        if duplicate_percentage >= 30:
            return 'critical'
        elif duplicate_percentage >= 15:
            return 'high'
        elif duplicate_percentage >= 5:
            return 'medium'
        else:
            return 'low'
