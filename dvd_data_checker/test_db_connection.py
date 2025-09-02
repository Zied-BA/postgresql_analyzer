#!/usr/bin/env python3
"""
Test database connection and extract real table information
"""

import psycopg2
import psycopg2.extras
import yaml
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from YAML file."""
    try:
        with open('config.yaml', 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print("Configuration file config.yaml not found.")
        return None

def test_connection_strategies(config):
    """Test different connection strategies."""
    
    # Try different database names
    database_names = ['dvdrental', 'dvd_rental', 'postgres']
    
    for db_name in database_names:
        logger.info(f"Trying database: {db_name}")
        
        # Strategy 1: Basic connection
        try:
            conn = psycopg2.connect(
                host=config['database']['host'],
                port=config['database']['port'],
                database=db_name,
                user=config['database']['user'],
                password=config['database']['password']
            )
            logger.info(f"✅ SUCCESS: Connected to database '{db_name}'")
            
            # Test getting schemas
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY schema_name
                """)
                schemas = [row[0] for row in cursor.fetchall()]
                logger.info(f"Available schemas: {schemas}")
                
                # Test getting tables for each schema
                for schema in schemas:
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = %s AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                    """, (schema,))
                    tables = [row[0] for row in cursor.fetchall()]
                    if tables:
                        logger.info(f"  Schema '{schema}': {tables}")
            
            conn.close()
            return db_name, schemas
            
        except Exception as e:
            logger.warning(f"❌ Failed to connect to '{db_name}': {e}")
            continue
    
    return None, []

def test_encoding_strategies(config, db_name):
    """Test different encoding strategies."""
    
    encoding_strategies = [
        # Strategy 1: UTF-8
        {'client_encoding': 'utf8'},
        # Strategy 2: Latin1
        {'client_encoding': 'latin1'},
        # Strategy 3: No encoding specified
        {},
        # Strategy 4: Connection string with encoding
        {'dsn': f"postgresql://{config['database']['user']}:{config['database']['password']}@{config['database']['host']}:{config['database']['port']}/{db_name}?client_encoding=utf8"},
        # Strategy 5: Connection string with latin1
        {'dsn': f"postgresql://{config['database']['user']}:{config['database']['password']}@{config['database']['host']}:{config['database']['port']}/{db_name}?client_encoding=latin1"}
    ]
    
    for i, strategy in enumerate(encoding_strategies, 1):
        try:
            if 'dsn' in strategy:
                conn = psycopg2.connect(strategy['dsn'])
            else:
                conn = psycopg2.connect(
                    host=config['database']['host'],
                    port=config['database']['port'],
                    database=db_name,
                    user=config['database']['user'],
                    password=config['database']['password'],
                    **{k: v for k, v in strategy.items() if k != 'dsn'}
                )
            
            logger.info(f"✅ Encoding strategy {i} successful")
            
            # Test a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                logger.info(f"PostgreSQL version: {version[:50]}...")
            
            conn.close()
            return strategy
            
        except Exception as e:
            logger.warning(f"❌ Encoding strategy {i} failed: {e}")
            continue
    
    return None

def main():
    """Main function to test database connection."""
    logger.info("Testing database connection...")
    
    # Load configuration
    config = load_config()
    if not config:
        return
    
    # Test different database names
    db_name, schemas = test_connection_strategies(config)
    
    if db_name:
        logger.info(f"🎉 Successfully connected to database: {db_name}")
        logger.info(f"Found schemas: {schemas}")
        
        # Test encoding strategies
        working_strategy = test_encoding_strategies(config, db_name)
        if working_strategy:
            logger.info(f"Working encoding strategy: {working_strategy}")
        
        # Update config with correct database name
        if db_name != config['database']['database']:
            logger.info(f"Updating config to use correct database name: {db_name}")
            config['database']['database'] = db_name
            
            # Save updated config
            with open('config.yaml', 'w') as file:
                yaml.dump(config, file, default_flow_style=False, indent=2)
            logger.info("✅ Configuration updated")
    else:
        logger.error("❌ Could not connect to any database")

if __name__ == "__main__":
    main()
