#!/usr/bin/env python3
"""
Script to verify the database configuration and ensure data persistence.
"""

import os
import sys
import sqlite3
from datetime import datetime

# Try to import db_manager
try:
    from db_manager import db, DatabaseManager
    print("Successfully imported database manager")
except ImportError:
    print("Error: Could not import database manager module. Make sure you're in the correct directory.")
    sys.exit(1)

def check_db_config():
    """Check the database configuration."""
    print("\n=== DATABASE CONFIGURATION CHECK ===")
    
    # Print database path
    print(f"Database path: {db.db_path}")
    
    # Check if it's an in-memory database
    if db.db_path == ":memory:":
        print("ERROR: Database is configured as in-memory! Data will not persist.")
        return False
    
    # Check if directory exists
    db_dir = os.path.dirname(os.path.abspath(db.db_path))
    print(f"Database directory: {db_dir}")
    print(f"Directory exists: {os.path.exists(db_dir)}")
    
    # Check if database file exists
    file_exists = os.path.exists(db.db_path)
    print(f"Database file exists: {file_exists}")
    
    if file_exists:
        file_size = os.path.getsize(db.db_path)
        print(f"Database file size: {file_size/1024:.2f} KB")
    
    # Test database connection
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check for tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print(f"Database contains {len(tables)} tables:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"  - {table[0]}: {count} records")
        
        conn.close()
        print("\nDatabase connection test: SUCCESS")
        return True
    except Exception as e:
        print(f"\nDatabase connection test: FAILED - {str(e)}")
        return False

def insert_test_record():
    """Insert a test record to verify database persistence."""
    print("\n=== INSERTING TEST RECORD ===")
    
    try:
        # Create a test job
        job_id = f"test-persistence-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        db.create_job(
            urls=["https://example.com/test"],
            prompts=["test_prompt"],
            name=f"DB Persistence Test {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        print(f"Test record created with job_id: {job_id}")
        print("Please check that this record persists after restarting your application.")
        return True
    except Exception as e:
        print(f"Error creating test record: {str(e)}")
        return False

if __name__ == "__main__":
    config_ok = check_db_config()
    
    if config_ok:
        insert_test_record()
        
        print("\n=== RECOMMENDATIONS ===")
        print("1. Your database appears to be correctly configured for persistence")
        print("2. Database file is located at:", db.db_path)
        print("3. Restart your application and verify the test record still exists")
    else:
        print("\n=== ACTION REQUIRED ===")
        print("Please update your db_manager.py with the provided modifications")

