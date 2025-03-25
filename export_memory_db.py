# save as export_memory_db.py
import os
import sqlite3
from db_manager import db

# Define where you want to save the database file
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
os.makedirs(data_dir, exist_ok=True)
output_db_path = os.path.join(data_dir, 'analysis_export.db')

print(f"Exporting in-memory database to: {output_db_path}")

try:
    # Open a connection to the in-memory database
    source_conn = db.get_connection()
    
    # Create a new database file
    dest_conn = sqlite3.connect(output_db_path)
    
    # Use the SQLite backup API to create a backup
    source_conn.backup(dest_conn)
    
    # Close connections
    dest_conn.close()
    source_conn.close()
    
    print(f"Successfully exported database to {output_db_path}")
    print("Your data is now safely stored in this file!")
    
except Exception as e:
    print(f"Error exporting database: {e}")
    import traceback
    print(traceback.format_exc())
