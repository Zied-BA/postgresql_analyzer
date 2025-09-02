"""
Date Gap Finder Module
Identifies time gaps in date/time columns and provides analysis reports.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from db.connector import DatabaseConnector
from db.schema_reader import SchemaReader


class DateGapFinder:
    """Identifies and analyzes time gaps in date/time columns."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initialize date gap finder with database connector.
        
        Args:
            db_connector: DatabaseConnector instance
        """
        self.db = db_connector
        self.schema_reader = SchemaReader(db_connector)
        self.logger = logging.getLogger(__name__)
    
    def find_gaps_in_date_column(self, table_name: str, date_column: str, 
                                schema: str = 'public', 
                                expected_interval: str = 'daily') -> Dict[str, Any]:
        """
        Find gaps in a date column based on expected interval.
        
        Args:
            table_name: Name of the table to analyze
            date_column: Name of the date column
            schema: Schema name (default: 'public')
            expected_interval: Expected interval ('daily', 'hourly', 'weekly', 'monthly')
            
        Returns:
            Dictionary containing gap analysis results
        """
        try:
            # Get date range for the column
            date_range_query = f"""
            SELECT 
                MIN({date_column}) as min_date,
                MAX({date_column}) as max_date,
                COUNT(*) as total_records
            FROM {schema}.{table_name}
            WHERE {date_column} IS NOT NULL
            """
            
            date_range = self.db.execute_query_dict(date_range_query)[0]
            min_date = date_range['min_date']
            max_date = date_range['max_date']
            total_records = date_range['total_records']
            
            if not min_date or not max_date:
                return {
                    'table_name': table_name,
                    'schema': schema,
                    'date_column': date_column,
                    'error': 'No valid dates found in column',
                    'severity': 'error'
                }
            
            # Find gaps based on expected interval
            gaps = self._find_gaps_for_interval(table_name, date_column, schema, 
                                              min_date, max_date, expected_interval)
            
            # Calculate gap statistics
            total_gaps = len(gaps)
            total_gap_days = sum(gap['gap_days'] for gap in gaps)
            
            gap_analysis = {
                'table_name': table_name,
                'schema': schema,
                'date_column': date_column,
                'expected_interval': expected_interval,
                'date_range': {
                    'min_date': min_date,
                    'max_date': max_date,
                    'total_days': (max_date - min_date).days
                },
                'total_records': total_records,
                'total_gaps': total_gaps,
                'total_gap_days': total_gap_days,
                'gaps': gaps,
                'severity': self._calculate_gap_severity(total_gaps, total_gap_days)
            }
            
            return gap_analysis
            
        except Exception as e:
            self.logger.error(f"Error finding gaps in {schema}.{table_name}.{date_column}: {e}")
            return {
                'table_name': table_name,
                'schema': schema,
                'date_column': date_column,
                'error': str(e),
                'severity': 'error'
            }
    
    def _find_gaps_for_interval(self, table_name: str, date_column: str, schema: str,
                               min_date: datetime, max_date: datetime, 
                               interval: str) -> List[Dict[str, Any]]:
        """
        Find gaps for a specific interval type.
        
        Args:
            table_name: Name of the table
            date_column: Name of the date column
            schema: Schema name
            min_date: Minimum date in the range
            max_date: Maximum date in the range
            interval: Interval type ('daily', 'hourly', 'weekly', 'monthly')
            
        Returns:
            List of gap dictionaries
        """
        gaps = []
        
        if interval == 'daily':
            # Find missing days
            gap_query = f"""
            WITH date_series AS (
                SELECT generate_series(
                    DATE({date_column}),
                    DATE({date_column}),
                    INTERVAL '1 day'
                )::date as expected_date
                FROM {schema}.{table_name}
                WHERE {date_column} IS NOT NULL
                GROUP BY DATE({date_column})
            ),
            actual_dates AS (
                SELECT DISTINCT DATE({date_column}) as actual_date
                FROM {schema}.{table_name}
                WHERE {date_column} IS NOT NULL
            )
            SELECT 
                ds.expected_date,
                ds.expected_date + INTERVAL '1 day' as next_expected_date,
                (ds.expected_date + INTERVAL '1 day' - ds.expected_date) as gap_days
            FROM date_series ds
            LEFT JOIN actual_dates ad ON ds.expected_date = ad.actual_date
            WHERE ad.actual_date IS NULL
            ORDER BY ds.expected_date
            """
            
        elif interval == 'hourly':
            # Find missing hours
            gap_query = f"""
            WITH hour_series AS (
                SELECT generate_series(
                    DATE_TRUNC('hour', {date_column}),
                    DATE_TRUNC('hour', {date_column}),
                    INTERVAL '1 hour'
                ) as expected_hour
                FROM {schema}.{table_name}
                WHERE {date_column} IS NOT NULL
                GROUP BY DATE_TRUNC('hour', {date_column})
            ),
            actual_hours AS (
                SELECT DISTINCT DATE_TRUNC('hour', {date_column}) as actual_hour
                FROM {schema}.{table_name}
                WHERE {date_column} IS NOT NULL
            )
            SELECT 
                hs.expected_hour,
                hs.expected_hour + INTERVAL '1 hour' as next_expected_hour,
                EXTRACT(EPOCH FROM (hs.expected_hour + INTERVAL '1 hour' - hs.expected_hour)) / 3600 as gap_hours
            FROM hour_series hs
            LEFT JOIN actual_hours ah ON hs.expected_hour = ah.actual_hour
            WHERE ah.actual_hour IS NULL
            ORDER BY hs.expected_hour
            """
            
        elif interval == 'weekly':
            # Find missing weeks
            gap_query = f"""
            WITH week_series AS (
                SELECT generate_series(
                    DATE_TRUNC('week', {date_column}),
                    DATE_TRUNC('week', {date_column}),
                    INTERVAL '1 week'
                ) as expected_week
                FROM {schema}.{table_name}
                WHERE {date_column} IS NOT NULL
                GROUP BY DATE_TRUNC('week', {date_column})
            ),
            actual_weeks AS (
                SELECT DISTINCT DATE_TRUNC('week', {date_column}) as actual_week
                FROM {schema}.{table_name}
                WHERE {date_column} IS NOT NULL
            )
            SELECT 
                ws.expected_week,
                ws.expected_week + INTERVAL '1 week' as next_expected_week,
                EXTRACT(EPOCH FROM (ws.expected_week + INTERVAL '1 week' - ws.expected_week)) / 86400 as gap_days
            FROM week_series ws
            LEFT JOIN actual_weeks aw ON ws.expected_week = aw.actual_week
            WHERE aw.actual_week IS NULL
            ORDER BY ws.expected_week
            """
            
        else:  # monthly
            # Find missing months
            gap_query = f"""
            WITH month_series AS (
                SELECT generate_series(
                    DATE_TRUNC('month', {date_column}),
                    DATE_TRUNC('month', {date_column}),
                    INTERVAL '1 month'
                ) as expected_month
                FROM {schema}.{table_name}
                WHERE {date_column} IS NOT NULL
                GROUP BY DATE_TRUNC('month', {date_column})
            ),
            actual_months AS (
                SELECT DISTINCT DATE_TRUNC('month', {date_column}) as actual_month
                FROM {schema}.{table_name}
                WHERE {date_column} IS NOT NULL
            )
            SELECT 
                ms.expected_month,
                ms.expected_month + INTERVAL '1 month' as next_expected_month,
                EXTRACT(EPOCH FROM (ms.expected_month + INTERVAL '1 month' - ms.expected_month)) / 86400 as gap_days
            FROM month_series ms
            LEFT JOIN actual_months am ON ms.expected_month = am.actual_month
            WHERE am.actual_month IS NULL
            ORDER BY ms.expected_month
            """
        
        try:
            gap_results = self.db.execute_query_dict(gap_query)
            
            for row in gap_results:
                if interval == 'hourly':
                    gap = {
                        'start_date': row['expected_hour'],
                        'end_date': row['next_expected_hour'],
                        'gap_days': row['gap_hours'] / 24,  # Convert hours to days
                        'gap_hours': row['gap_hours']
                    }
                else:
                    gap = {
                        'start_date': row['expected_date'] if 'expected_date' in row else row['expected_week'] if 'expected_week' in row else row['expected_month'],
                        'end_date': row['next_expected_date'] if 'next_expected_date' in row else row['next_expected_week'] if 'next_expected_week' in row else row['next_expected_month'],
                        'gap_days': row['gap_days']
                    }
                
                gaps.append(gap)
                
        except Exception as e:
            self.logger.warning(f"Could not find gaps for {interval} interval: {e}")
        
        return gaps
    
    def find_gaps_in_table(self, table_name: str, schema: str = 'public') -> Dict[str, Any]:
        """
        Find gaps in all date columns of a table.
        
        Args:
            table_name: Name of the table to analyze
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary containing gap analysis for all date columns
        """
        try:
            # Get date columns in the table
            date_columns = self.schema_reader.get_date_columns(table_name, schema)
            
            if not date_columns:
                return {
                    'table_name': table_name,
                    'schema': schema,
                    'date_columns_found': 0,
                    'message': 'No date columns found in table'
                }
            
            table_analysis = {
                'table_name': table_name,
                'schema': schema,
                'date_columns_found': len(date_columns),
                'column_analyses': [],
                'total_gaps': 0,
                'severity': 'low'
            }
            
            for column in date_columns:
                column_name = column['column_name']
                data_type = column['data_type']
                
                # Determine expected interval based on data type and column name
                expected_interval = self._determine_expected_interval(column_name, data_type)
                
                column_analysis = self.find_gaps_in_date_column(
                    table_name, column_name, schema, expected_interval
                )
                
                table_analysis['column_analyses'].append(column_analysis)
                table_analysis['total_gaps'] += column_analysis.get('total_gaps', 0)
            
            # Determine overall severity
            severities = [analysis.get('severity', 'low') for analysis in table_analysis['column_analyses']]
            if 'critical' in severities:
                table_analysis['severity'] = 'critical'
            elif 'high' in severities:
                table_analysis['severity'] = 'high'
            elif 'medium' in severities:
                table_analysis['severity'] = 'medium'
            
            return table_analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing gaps in table {schema}.{table_name}: {e}")
            return {
                'table_name': table_name,
                'schema': schema,
                'error': str(e),
                'severity': 'error'
            }
    
    def find_gaps_in_schema(self, schema: str = 'public') -> Dict[str, Any]:
        """
        Find gaps in all tables with date columns in a schema.
        
        Args:
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary containing gap analysis for all tables
        """
        try:
            tables = self.schema_reader.get_tables_by_schema(schema)
            
            schema_analysis = {
                'schema': schema,
                'total_tables': len(tables),
                'tables_analyzed': 0,
                'tables_with_gaps': 0,
                'critical_issues': 0,
                'high_issues': 0,
                'medium_issues': 0,
                'low_issues': 0,
                'table_results': []
            }
            
            for table in tables:
                table_name = table['table_name']
                self.logger.info(f"Analyzing date gaps in {schema}.{table_name}")
                
                table_analysis = self.find_gaps_in_table(table_name, schema)
                schema_analysis['table_results'].append(table_analysis)
                schema_analysis['tables_analyzed'] += 1
                
                if table_analysis.get('severity') != 'error':
                    if table_analysis.get('total_gaps', 0) > 0:
                        schema_analysis['tables_with_gaps'] += 1
                    
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
        Run date gap analysis on specified schemas.
        
        Args:
            schemas: List of schema names to analyze (default: all schemas)
            
        Returns:
            List of analysis results for each schema
        """
        if schemas is None:
            schemas = [schema['schema_name'] for schema in self.schema_reader.get_schemas()]
        
        results = []
        
        for schema in schemas:
            self.logger.info(f"Starting date gap analysis for schema: {schema}")
            schema_result = self.find_gaps_in_schema(schema)
            results.append(schema_result)
        
        return results
    
    def generate_gap_report(self, analysis_results: List[Dict[str, Any]]) -> str:
        """
        Generate a human-readable report from date gap analysis results.
        
        Args:
            analysis_results: Results from date gap analysis
            
        Returns:
            Formatted report string
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("DATE GAP ANALYSIS REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        total_critical = 0
        total_high = 0
        total_medium = 0
        total_low = 0
        total_gaps = 0
        
        for schema_result in analysis_results:
            if 'error' in schema_result:
                report_lines.append(f"ERROR in schema {schema_result['schema']}: {schema_result['error']}")
                report_lines.append("")
                continue
            
            schema = schema_result['schema']
            report_lines.append(f"SCHEMA: {schema}")
            report_lines.append("-" * 40)
            report_lines.append(f"Tables analyzed: {schema_result['tables_analyzed']}")
            report_lines.append(f"Tables with gaps: {schema_result['tables_with_gaps']}")
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
                
                if table_result.get('total_gaps', 0) > 0:
                    report_lines.append(f"  Table: {table_result['table_name']} (Severity: {table_result['severity'].upper()})")
                    report_lines.append(f"    Date columns analyzed: {table_result['date_columns_found']}")
                    report_lines.append(f"    Total gaps found: {table_result['total_gaps']}")
                    
                    for column_analysis in table_result['column_analyses']:
                        if column_analysis.get('total_gaps', 0) > 0:
                            report_lines.append(f"    - {column_analysis['date_column']}: {column_analysis['total_gaps']} gaps")
                            report_lines.append(f"      Expected interval: {column_analysis['expected_interval']}")
                            report_lines.append(f"      Date range: {column_analysis['date_range']['min_date']} to {column_analysis['date_range']['max_date']}")
                    
                    total_gaps += table_result['total_gaps']
                    report_lines.append("")
            
            report_lines.append("")
        
        # Overall summary
        report_lines.append("=" * 80)
        report_lines.append("OVERALL SUMMARY")
        report_lines.append("=" * 80)
        report_lines.append(f"Total gaps found: {total_gaps}")
        report_lines.append(f"Critical issues: {total_critical}")
        report_lines.append(f"High issues: {total_high}")
        report_lines.append(f"Medium issues: {total_medium}")
        report_lines.append(f"Low issues: {total_low}")
        
        if total_critical > 0:
            report_lines.append("\n🚨 CRITICAL: Large date gaps detected - data continuity issues!")
        elif total_high > 0:
            report_lines.append("\n⚠️  HIGH: Significant date gaps found")
        elif total_medium > 0:
            report_lines.append("\n⚠️  MEDIUM: Some date gaps detected")
        else:
            report_lines.append("\n✅ No significant date gaps found")
        
        return "\n".join(report_lines)
    
    def _determine_expected_interval(self, column_name: str, data_type: str) -> str:
        """
        Determine expected interval based on column name and data type.
        
        Args:
            column_name: Name of the date column
            data_type: PostgreSQL data type
            
        Returns:
            Expected interval string
        """
        column_lower = column_name.lower()
        
        if 'hour' in column_lower or data_type in ['timestamp', 'timestamp without time zone', 'timestamp with time zone']:
            return 'hourly'
        elif 'week' in column_lower:
            return 'weekly'
        elif 'month' in column_lower:
            return 'monthly'
        else:
            return 'daily'
    
    def _calculate_gap_severity(self, total_gaps: int, total_gap_days: float) -> str:
        """
        Calculate severity level based on gap statistics.
        
        Args:
            total_gaps: Total number of gaps
            total_gap_days: Total number of days in gaps
            
        Returns:
            Severity level (critical, high, medium, low)
        """
        if total_gaps >= 10 or total_gap_days >= 30:
            return 'critical'
        elif total_gaps >= 5 or total_gap_days >= 7:
            return 'high'
        elif total_gaps >= 2 or total_gap_days >= 3:
            return 'medium'
        else:
            return 'low'
