# Schema and Table Selection Guide

## Overview

The DVD Data Checker now supports analyzing specific schemas and tables in your PostgreSQL database. This allows you to:

- Analyze specific tables instead of entire schemas
- Work with multiple schemas in your database
- Focus analysis on particular areas of interest
- Improve performance by limiting scope

## Basic Commands

### List Available Schemas
```bash
python main.py --list-schemas
```

### List Tables in a Schema
```bash
python main.py --list-tables --schema public
```

### Analyze Specific Table
```bash
# Check missing values in a specific table
python main.py --check-missing --schema public --table customer

# Check duplicates in a specific table
python main.py --check-duplicates --schema public --table rental

# Find date gaps in a specific table
python main.py --find-gaps --schema public --table rental
```

### Analyze All Tables in a Schema
```bash
# Check missing values in all tables in public schema
python main.py --check-missing --schema public

# Run multiple analyses on all tables in public schema
python main.py --check-missing --check-duplicates --find-gaps --schema public
```

### Generate Comprehensive Report
```bash
# For a specific table
python main.py --generate-report --schema public --table payment

# For all tables in a schema
python main.py --generate-report --schema public
```

## Configuration

### Default Settings
In `config.yaml`, you can set default schema and table:

```yaml
analysis:
  default_schema: "public"
  default_table: null  # null means analyze all tables
  
  # Available schemas for analysis
  schemas:
    - "public"
    - "custom_schema1"
    - "custom_schema2"
  
  # Tables to exclude from analysis
  exclude_tables:
    - "pg_*"
    - "*_temp"
    - "*_backup"
```

### Command Line Override
Command line arguments override configuration defaults:

```bash
# Use different schema than configured default
python main.py --check-missing --schema custom_schema

# Analyze specific table regardless of config
python main.py --check-missing --table specific_table
```

## Examples for DVD Rental Database

### Common DVD Rental Tables
```bash
# Customer data analysis
python main.py --check-missing --schema public --table customer
python main.py --check-duplicates --schema public --table customer

# Rental data analysis
python main.py --check-missing --schema public --table rental
python main.py --find-gaps --schema public --table rental

# Payment data analysis
python main.py --check-missing --schema public --table payment
python main.py --check-duplicates --schema public --table payment

# Film data analysis
python main.py --check-missing --schema public --table film
python main.py --check-duplicates --schema public --table film
```

### Business Logic (No Schema/Table Needed)
```bash
# DVD return checking (uses business logic)
python main.py --check-returns

# Email preparation (uses business logic)
python main.py --prepare-emails
```

## Performance Tips

1. **Analyze specific tables** when you know which ones have issues
2. **Use schema filtering** to avoid system tables
3. **Combine multiple analyses** in one command for efficiency
4. **Exclude temporary tables** in configuration

## Error Handling

- If schema doesn't exist: Error message with available schemas
- If table doesn't exist: Error message with available tables in schema
- If no tables found: Informational message
- Connection issues: Detailed error with connection parameters

## Advanced Usage

### Multiple Schemas
```bash
# Analyze tables across multiple schemas
python main.py --check-missing --schema public
python main.py --check-missing --schema custom_schema
```

### Pattern Matching
```bash
# List tables matching a pattern (if supported by your database)
python main.py --list-tables --schema public | grep "customer"
```

### Batch Processing
```bash
# Create a script to analyze multiple tables
for table in customer rental payment film; do
    python main.py --check-missing --schema public --table $table
done
```
