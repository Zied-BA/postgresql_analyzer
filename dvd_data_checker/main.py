#!/usr/bin/env python3
"""
DVD Data Checker - Main Application
A PostgreSQL data analysis tool for DVD rental business data quality checks.
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from db.connector import DatabaseConnector
from analysis.missing_checker import MissingValueChecker
from analysis.duplicate_checker import DuplicateChecker
from analysis.date_gap_finder import DateGapFinder
from analysis.regression_generator import RegressionGenerator
from use_cases.dvd_return_check import DVDReturnChecker
from use_cases.email_preparer import EmailPreparer
import yaml


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('dvd_data_checker.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def load_config(config_path='config.yaml'):
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Configuration file {config_path} not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing configuration file: {e}")
        sys.exit(1)


def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(description='DVD Data Checker - PostgreSQL Data Analysis Tool')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path')
    
    # Schema and table selection
    parser.add_argument('--schema', help='Schema name to analyze (default: from config)')
    parser.add_argument('--table', help='Specific table name to analyze (default: all tables in schema)')
    parser.add_argument('--list-schemas', action='store_true', help='List available schemas')
    parser.add_argument('--list-tables', action='store_true', help='List available tables in schema')
    
    # Analysis options
    parser.add_argument('--check-missing', action='store_true', help='Check for missing values')
    parser.add_argument('--check-duplicates', action='store_true', help='Check for duplicates')
    parser.add_argument('--find-gaps', action='store_true', help='Find date gaps in data')
    parser.add_argument('--check-returns', action='store_true', help='Check DVD returns')
    parser.add_argument('--generate-report', action='store_true', help='Generate comprehensive report')
    parser.add_argument('--prepare-emails', action='store_true', help='Prepare warning emails')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    logger.info("Starting DVD Data Checker")
    
    # Load configuration
    config = load_config(args.config)
    
    try:
        # Initialize database connection
        db_connector = DatabaseConnector(config['database'])
        
        # Handle schema and table listing
        if args.list_schemas:
            logger.info("Available schemas:")
            config_schemas = config['analysis'].get('schemas', [])
            schemas = db_connector.get_schemas_safe(config_schemas)
            for schema in schemas:
                print(f"  - {schema}")
            return
        
        if args.list_tables:
            schema_name = args.schema or config['analysis']['default_schema']
            logger.info(f"Available tables in schema '{schema_name}':")
            tables = db_connector.get_tables_by_schema(schema_name)
            for table in tables:
                print(f"  - {table}")
            return
        
        # Determine schema and table for analysis
        schema_name = args.schema or config['analysis']['default_schema']
        table_name = args.table or config['analysis']['default_table']
        
        logger.info(f"Analysis target: Schema='{schema_name}', Table='{table_name or 'ALL TABLES'}'")
        
        # Run requested analyses
        if args.check_missing or args.generate_report:
            logger.info("Checking for missing values...")
            missing_checker = MissingValueChecker(db_connector)
            if table_name:
                missing_report = missing_checker.check_table_missing_values(table_name, schema_name)
            else:
                missing_report = missing_checker.check_schema_missing_values(schema_name)
            logger.info(f"Missing values analysis complete: {len(missing_report)} tables with issues")
        
        if args.check_duplicates or args.generate_report:
            logger.info("Checking for duplicates...")
            duplicate_checker = DuplicateChecker(db_connector)
            if table_name:
                duplicate_report = duplicate_checker.check_table_duplicates(schema_name, table_name)
            else:
                duplicate_report = duplicate_checker.check_schema_duplicates(schema_name)
            logger.info(f"Duplicate analysis complete: {len(duplicate_report)} tables with duplicates")
        
        if args.find_gaps or args.generate_report:
            logger.info("Finding date gaps...")
            gap_finder = DateGapFinder(db_connector)
            if table_name:
                gap_report = gap_finder.find_gaps_in_table(schema_name, table_name)
            else:
                gap_report = gap_finder.find_gaps_in_schema(schema_name)
            logger.info(f"Date gap analysis complete: {len(gap_report)} gaps found")
        
        if args.check_returns:
            logger.info("Checking DVD returns...")
            return_checker = DVDReturnChecker(db_connector)
            return_report = return_checker.check_missing_returns()
            logger.info(f"Return check complete: {len(return_report)} customers with missing returns")
        
        if args.prepare_emails:
            logger.info("Preparing warning emails...")
            email_preparer = EmailPreparer(db_connector)
            email_report = email_preparer.prepare_warning_emails()
            logger.info(f"Email preparation complete: {len(email_report)} emails prepared")
        
        logger.info("DVD Data Checker completed successfully")
        
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
