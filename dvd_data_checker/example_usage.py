#!/usr/bin/env python3
"""
Example Usage Script for DVD Data Checker
Demonstrates how to use schema and table selection features.
"""

import subprocess
import sys
import os

def run_command(command):
    """Run a command and print the output."""
    print(f"\n{'='*60}")
    print(f"Running: {command}")
    print('='*60)
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        print(f"Exit code: {result.returncode}")
    except Exception as e:
        print(f"Error running command: {e}")

def main():
    """Demonstrate various usage examples."""
    
    print("DVD Data Checker - Schema and Table Selection Examples")
    print("=" * 60)
    
    # Example 1: List all available schemas
    print("\n1. List all available schemas:")
    run_command("python main.py --list-schemas")
    
    # Example 2: List all tables in the public schema
    print("\n2. List all tables in the public schema:")
    run_command("python main.py --list-tables --schema public")
    
    # Example 3: Check missing values in a specific table
    print("\n3. Check missing values in a specific table:")
    run_command("python main.py --check-missing --schema public --table customer")
    
    # Example 4: Check duplicates in a specific table
    print("\n4. Check duplicates in a specific table:")
    run_command("python main.py --check-duplicates --schema public --table rental")
    
    # Example 5: Find date gaps in a specific table
    print("\n5. Find date gaps in a specific table:")
    run_command("python main.py --find-gaps --schema public --table rental")
    
    # Example 6: Analyze all tables in a specific schema
    print("\n6. Analyze all tables in a specific schema:")
    run_command("python main.py --check-missing --check-duplicates --find-gaps --schema public")
    
    # Example 7: Generate comprehensive report for a specific table
    print("\n7. Generate comprehensive report for a specific table:")
    run_command("python main.py --generate-report --schema public --table payment")
    
    # Example 8: Check DVD returns (business logic)
    print("\n8. Check DVD returns (business logic):")
    run_command("python main.py --check-returns")
    
    # Example 9: Prepare warning emails
    print("\n9. Prepare warning emails:")
    run_command("python main.py --prepare-emails")

if __name__ == "__main__":
    # Change to the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    main()
