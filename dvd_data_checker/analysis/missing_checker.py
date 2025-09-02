"""
Missing Value Checker Module
Analyzes tables for missing values and provides detailed reports.
"""

import logging
from typing import Dict, List, Any, Optional
from db.connector import DatabaseConnector
from db.schema_reader import SchemaReader


class MissingValueChecker:
    """Analyzes database tables for missing values and data quality issues."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initialize missing value checker with database connector.
        
        Args:
            db_connector: DatabaseConnector instance
        """
        self.db = db_connector
        self.schema_reader = SchemaReader(db_connector)
        self.logger = logging.getLogger(__name__)
    
    def check_table_missing_values(self, table_name: str, schema: str = 'public') -> Dict[str, Any]:
        """
        Check for missing values in a specific table.
        
        Args:
            table_name: Name of the table to analyze
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary containing missing value analysis results
        """
        try:
            # Get table structure
            table_structure = self.schema_reader.get_table_structure(table_name, schema)
            columns = table_structure['columns']
            
            missing_analysis = {
                'table_name': table_name,
                'schema': schema,
                'total_rows': table_structure['row_count'],
                'columns_analyzed': len(columns),
                'columns_with_missing': 0,
                'missing_details': [],
                'severity': 'low'
            }
            
            for column in columns:
                column_name = column['column_name']
                data_type = column['data_type']
                is_nullable = column['is_nullable'] == 'YES'
                
                # Skip columns that are designed to be nullable
                if is_nullable:
                    continue
                
                # Count NULL values in the column
                null_count_query = f"SELECT COUNT(*) FROM {schema}.{table_name} WHERE {column_name} IS NULL"
                null_count = self.db.execute_query(null_count_query)[0][0]
                
                if null_count > 0:
                    missing_analysis['columns_with_missing'] += 1
                    
                    # Calculate percentage of missing values
                    total_rows = table_structure['row_count']
                    missing_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
                    
                    column_detail = {
                        'column_name': column_name,
                        'data_type': data_type,
                        'null_count': null_count,
                        'missing_percentage': round(missing_percentage, 2),
                        'is_nullable': is_nullable,
                        'severity': self._calculate_severity(missing_percentage)
                    }
                    
                    missing_analysis['missing_details'].append(column_detail)
            
            # Determine overall severity
            if missing_analysis['missing_details']:
                severities = [detail['severity'] for detail in missing_analysis['missing_details']]
                if 'critical' in severities:
                    missing_analysis['severity'] = 'critical'
                elif 'high' in severities:
                    missing_analysis['severity'] = 'high'
                elif 'medium' in severities:
                    missing_analysis['severity'] = 'medium'
                else:
                    missing_analysis['severity'] = 'low'
            
            return missing_analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing missing values in {schema}.{table_name}: {e}")
            return {
                'table_name': table_name,
                'schema': schema,
                'error': str(e),
                'severity': 'error'
            }
    
    def check_schema_missing_values(self, schema: str = 'public') -> Dict[str, Any]:
        """
        Check for missing values across all tables in a schema.
        
        Args:
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary containing missing value analysis for all tables
        """
        try:
            tables = self.schema_reader.get_tables_by_schema(schema)
            
            schema_analysis = {
                'schema': schema,
                'total_tables': len(tables),
                'tables_analyzed': 0,
                'tables_with_missing': 0,
                'critical_issues': 0,
                'high_issues': 0,
                'medium_issues': 0,
                'low_issues': 0,
                'table_results': []
            }
            
            for table in tables:
                table_name = table['table_name']
                self.logger.info(f"Analyzing missing values in {schema}.{table_name}")
                
                table_analysis = self.check_table_missing_values(table_name, schema)
                schema_analysis['table_results'].append(table_analysis)
                schema_analysis['tables_analyzed'] += 1
                
                if table_analysis.get('severity') != 'error':
                    if table_analysis['columns_with_missing'] > 0:
                        schema_analysis['tables_with_missing'] += 1
                    
                    # Count issues by severity
                    severity = table_analysis.get('severity', 'low')
                    if severity == 'critical':
                        schema_analysis['critical_issues'] += 1
                    elif severity == 'high':
                        schema_analysis['high_issues'] += 1
                    elif severity == 'medium':
                        schema_analysis['medium_issues'] += 1
                    elif severity == 'low':
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
        Run missing value analysis on specified schemas.
        
        Args:
            schemas: List of schema names to analyze (default: all schemas)
            
        Returns:
            List of analysis results for each schema
        """
        if schemas is None:
            schemas = [schema['schema_name'] for schema in self.schema_reader.get_schemas()]
        
        results = []
        
        for schema in schemas:
            self.logger.info(f"Starting missing value analysis for schema: {schema}")
            schema_result = self.check_schema_missing_values(schema)
            results.append(schema_result)
        
        return results
    
    def generate_missing_data_report(self, analysis_results: List[Dict[str, Any]]) -> str:
        """
        Generate a human-readable report from missing value analysis results.
        
        Args:
            analysis_results: Results from missing value analysis
            
        Returns:
            Formatted report string
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("MISSING VALUE ANALYSIS REPORT")
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
            report_lines.append(f"Tables with missing values: {schema_result['tables_with_missing']}")
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
                if table_result.get('severity') == 'error':
                    report_lines.append(f"  ERROR in table {table_result['table_name']}: {table_result['error']}")
                    continue
                
                if table_result['columns_with_missing'] > 0:
                    report_lines.append(f"  Table: {table_result['table_name']} (Severity: {table_result['severity'].upper()})")
                    report_lines.append(f"    Total rows: {table_result['total_rows']}")
                    report_lines.append(f"    Columns with missing values: {table_result['columns_with_missing']}")
                    
                    for column_detail in table_result['missing_details']:
                        report_lines.append(f"    - {column_detail['column_name']}: {column_detail['null_count']} NULL values ({column_detail['missing_percentage']}%)")
                    
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
            report_lines.append("\n⚠️  CRITICAL: Immediate attention required!")
        elif total_high > 0:
            report_lines.append("\n⚠️  HIGH: Priority attention recommended")
        elif total_medium > 0:
            report_lines.append("\n⚠️  MEDIUM: Review recommended")
        else:
            report_lines.append("\n✅ No critical or high priority issues found")
        
        return "\n".join(report_lines)
    
    def _calculate_severity(self, missing_percentage: float) -> str:
        """
        Calculate severity level based on missing value percentage.
        
        Args:
            missing_percentage: Percentage of missing values
            
        Returns:
            Severity level (critical, high, medium, low)
        """
        if missing_percentage >= 50:
            return 'critical'
        elif missing_percentage >= 20:
            return 'high'
        elif missing_percentage >= 5:
            return 'medium'
        else:
            return 'low'
    
    def find_critical_missing_data(self, analysis_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Find tables with critical missing data issues.
        
        Args:
            analysis_results: Results from missing value analysis
            
        Returns:
            List of critical issues
        """
        critical_issues = []
        
        for schema_result in analysis_results:
            if 'error' in schema_result:
                continue
            
            for table_result in schema_result['table_results']:
                if table_result.get('severity') == 'critical':
                    critical_issues.append({
                        'schema': schema_result['schema'],
                        'table': table_result['table_name'],
                        'total_rows': table_result['total_rows'],
                        'missing_columns': table_result['columns_with_missing'],
                        'details': table_result['missing_details']
                    })
        
        return critical_issues
