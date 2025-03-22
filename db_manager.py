#!/usr/bin/env python3
"""
Database Manager Module

This module provides a SQLite database interface for storing analysis jobs,
results, and other application data. It handles database initialization,
queries, and data export functionality.
"""

import os
import sqlite3
import json
import time
import csv
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("db_manager")

# Default database location
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "analysis.db")

class DatabaseManager:
    """
    Manages database operations for the content analysis application.
    """
    
    def __init__(self, db_path: str = None):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file
        """
        if not db_path:
            # Default database location in data directory
            data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, 'analysis.db')
                
        self.db_path = db_path
        self.init_db()  # Changed from self._init_db() to self.init_db()
        self.db = self  # Add this line - this will make self.db.method() work by referring to self
        logger.info(f"Database initialized at {db_path}")
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a database connection with row factory for dict-like access.
        
        Returns:
            A SQLite connection object
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_db(self) -> None:
        """
        Initialize the database schema if it doesn't exist.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Create jobs table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT,
            created_at REAL,
            updated_at REAL,
            completed_at REAL,
            total_urls INTEGER,
            processed_urls INTEGER,
            error_count INTEGER,
            prompt_names TEXT,
            company_info TEXT
        )
        ''')
        
        # Create results table for storing analysis results
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            url TEXT,
            title TEXT,
            status TEXT,
            content_type TEXT,
            word_count INTEGER,
            processed_at REAL,
            prompt_name TEXT,
            api_tokens INTEGER,
            error TEXT,
            data TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
        ''')
        
        # Create prompt_usage table to track prompt usage
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS prompt_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt_name TEXT,
            job_id TEXT,
            url TEXT,
            tokens_used INTEGER,
            processed_at REAL,
            success BOOLEAN,
            FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
        ''')
        
        # Create indexes for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_results_job_id ON results (job_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_results_url ON results (url)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prompt_usage_job_id ON prompt_usage (job_id)')
        
        conn.commit()
        conn.close()
    
    def create_job(self, job_id: str = None, name: str = "", prompt_names: List[str] = None, 
              company_info: Dict[str, Any] = None, urls: List[str] = None, 
              prompts: List[str] = None) -> str:
        """
        Create a new analysis job record.
        
        Args:
            job_id: Unique identifier for the job (generated if not provided)
            name: Optional name for the job
            prompt_names: List of prompt names being used
            company_info: Optional company context information
            urls: List of URLs to analyze (backward compatibility)
            prompts: List of prompt names (backward compatibility)
            
        Returns:
            The job ID
        """
        try:
            # For backward compatibility
            if job_id is None:
                job_id = str(uuid.uuid4())
                
            # Handle old parameter names
            if prompt_names is None and prompts is not None:
                prompt_names = prompts
                
            # Convert lists and dicts to JSON strings
            prompt_names_json = json.dumps(prompt_names or [])
            company_info_json = json.dumps(company_info or {})
            
            # Set total_urls if 'urls' parameter is provided
            total_urls = len(urls) if urls is not None else 0
            
            current_time = time.time()
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO jobs (
                job_id, name, status, created_at, updated_at, 
                total_urls, processed_urls, error_count, 
                prompt_names, company_info
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_id, name, 'pending', current_time, current_time,
                total_urls, 0, 0, prompt_names_json, company_info_json
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Created new job with ID: {job_id}")
            return job_id
                
        except Exception as e:
            logger.error(f"Error creating job {job_id}: {str(e)}")
            return job_id

    # Still return job_id for backward compatibility
    
    def update_job_status(self, job_id: str, status: str = None, 
                     processed_urls: Optional[int] = None,
                     total_urls: Optional[int] = None,
                     error_count: Optional[int] = None,
                     completed_at: Optional[Union[float, str]] = None) -> bool:
        """
        Update the status and counters of a job.
        
        Args:
            job_id: The job identifier
            status: New status value
            processed_urls: Number of processed URLs
            total_urls: Total number of URLs
            error_count: Number of errors
            completed_at: Timestamp when job completed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get current job data
            cursor.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,))
            job = cursor.fetchone()
            
            if not job:
                logger.warning(f"Job {job_id} not found when updating status")
                return False
            
            update_fields = ['updated_at = ?']
            params = [time.time()]
            
            if status is not None:
                update_fields.append('status = ?')
                params.append(status)
                    
            if processed_urls is not None:
                update_fields.append('processed_urls = ?')
                params.append(processed_urls)
                    
            if total_urls is not None:
                update_fields.append('total_urls = ?')
                params.append(total_urls)
                    
            if error_count is not None:
                update_fields.append('error_count = ?')
                params.append(error_count)
                    
            # If completed_at is provided or status is 'completed'/'completed_with_errors'/'failed'
            if completed_at is not None:
                update_fields.append('completed_at = ?')
                # Handle string timestamp or use the value directly
                if isinstance(completed_at, str):
                    try:
                        dt = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
                        params.append(dt.timestamp())
                    except ValueError:
                        params.append(time.time())
                else:
                    params.append(completed_at)
            elif status in ['completed', 'completed_with_errors', 'failed']:
                update_fields.append('completed_at = ?')
                params.append(time.time())
            
            # Build the update query
            query = f"UPDATE jobs SET {', '.join(update_fields)} WHERE job_id = ?"
            params.append(job_id)
            
            cursor.execute(query, params)
            conn.commit()
            conn.close()
            
            logger.info(f"Updated job {job_id} status to {status}")
            return True
                
        except Exception as e:
            logger.error(f"Error updating job {job_id} status: {str(e)}")
            return False
    
    def save_result(self, job_id: str, result: Dict[str, Any]) -> int:
        """
        Save a result from content analysis.
        
        Args:
            job_id: The associated job identifier
            result: Result data dictionary
            
        Returns:
            ID of the saved result or -1 on error
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Extract basic fields
            url = result.get('url', '')
            title = result.get('title', '')
            status = result.get('status', 'success')
            content_type = result.get('content_type', 'html')
            word_count = result.get('word_count', 0)
            prompt_name = result.get('prompt_name', '')
            api_tokens = result.get('api_tokens', 0)
            error = result.get('error', '')
            
            # Convert full result to JSON
            result_json = json.dumps(result)
            
            # Check if this URL already exists for this job
            cursor.execute(
                'SELECT id FROM results WHERE job_id = ? AND url = ?',
                (job_id, url)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update existing record
                cursor.execute('''
                UPDATE results SET 
                    title = ?, status = ?, content_type = ?,
                    word_count = ?, processed_at = ?, prompt_name = ?,
                    api_tokens = ?, error = ?, data = ?
                WHERE id = ?
                ''', (
                    title, status, content_type, word_count,
                    time.time(), prompt_name, api_tokens, error,
                    result_json, existing['id']
                ))
                
                result_id = existing['id']
                logger.info(f"Updated result for job {job_id}, URL: {url}")
                
            else:
                # Insert new record
                cursor.execute('''
                INSERT INTO results (
                    job_id, url, title, status, content_type,
                    word_count, processed_at, prompt_name,
                    api_tokens, error, data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job_id, url, title, status, content_type,
                    word_count, time.time(), prompt_name,
                    api_tokens, error, result_json
                ))
                
                result_id = cursor.lastrowid
                logger.info(f"Inserted new result for job {job_id}, URL: {url}")
            
            # Record prompt usage
            if prompt_name and api_tokens > 0:
                cursor.execute('''
                INSERT INTO prompt_usage (
                    prompt_name, job_id, url, tokens_used, 
                    processed_at, success
                ) VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    prompt_name, job_id, url, api_tokens,
                    time.time(), status == 'success'
                ))
            
            conn.commit()
            conn.close()
            
            return result_id
            
        except Exception as e:
            logger.error(f"Error saving result: {str(e)}")
            return -1
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job details by ID.
        
        Args:
            job_id: The job identifier
            
        Returns:
            Job details or None if not found
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,))
            job = cursor.fetchone()
            
            if not job:
                return None
            
            # Convert to dict
            job_dict = dict(job)
            
            # Parse JSON fields
            if 'prompt_names' in job_dict and job_dict['prompt_names']:
                job_dict['prompt_names'] = json.loads(job_dict['prompt_names'])
            else:
                job_dict['prompt_names'] = []
                
            if 'company_info' in job_dict and job_dict['company_info']:
                job_dict['company_info'] = json.loads(job_dict['company_info'])
            else:
                job_dict['company_info'] = {}
            
            # Calculate progress percentage
            total = job_dict.get('total_urls', 0)
            processed = job_dict.get('processed_urls', 0)
            
            if total > 0:
                job_dict['progress'] = (processed / total) * 100
            else:
                job_dict['progress'] = 0
            
            # Add formatted timestamps
            for ts_field in ['created_at', 'updated_at', 'completed_at']:
                if job_dict.get(ts_field):
                    job_dict[f"{ts_field}_formatted"] = datetime.fromtimestamp(
                        job_dict[ts_field]).strftime('%Y-%m-%d %H:%M:%S')
            
            conn.close()
            return job_dict
            
        except Exception as e:
            logger.error(f"Error getting job {job_id}: {str(e)}")
            return None
    
    
    
    
    def get_all_jobs(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get all jobs with pagination.
        
        Args:
            limit: Maximum number of jobs to return
            offset: Starting offset for pagination
            
        Returns:
            List of job dictionaries
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT * FROM jobs 
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            ''', (limit, offset))
            
            jobs = []
            
            for row in cursor.fetchall():
                job = dict(row)
                
                # Parse JSON fields
                if 'prompt_names' in job and job['prompt_names']:
                    job['prompt_names'] = json.loads(job['prompt_names'])
                else:
                    job['prompt_names'] = []
                    
                if 'company_info' in job and job['company_info']:
                    job['company_info'] = json.loads(job['company_info'])
                else:
                    job['company_info'] = {}
                
                # Calculate progress percentage
                total = job.get('total_urls', 0)
                processed = job.get('processed_urls', 0)
                
                if total > 0:
                    job['progress'] = (processed / total) * 100
                else:
                    job['progress'] = 0
                
                # Add formatted timestamps
                for ts_field in ['created_at', 'updated_at', 'completed_at']:
                    if job.get(ts_field):
                        job[f"{ts_field}_formatted"] = datetime.fromtimestamp(
                            job[ts_field]).strftime('%Y-%m-%d %H:%M:%S')
                
                jobs.append(job)
            
            conn.close()
            return jobs
            
        except Exception as e:
            logger.error(f"Error getting all jobs: {str(e)}")
            return []
    
    def get_job_metrics(self, job_id: str) -> Dict[str, Any]:
        """
        Get metrics for a specific job.
        
        Args:
            job_id: The job identifier
            
        Returns:
            Dictionary of job metrics
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get job basics
            job = self.get_job(job_id)
            
            if not job:
                return {"error": "Job not found"}
            
            # Get result stats
            cursor.execute('''
            SELECT 
                COUNT(*) as total_results,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed,
                SUM(word_count) as total_words,
                SUM(api_tokens) as total_tokens
            FROM results 
            WHERE job_id = ?
            ''', (job_id,))
            
            result_stats = dict(cursor.fetchone())
            
            # Get prompt usage stats
            cursor.execute('''
            SELECT 
                prompt_name,
                COUNT(*) as usage_count,
                SUM(tokens_used) as tokens_used 
            FROM prompt_usage 
            WHERE job_id = ? 
            GROUP BY prompt_name
            ''', (job_id,))
            
            prompt_usage = []
            for row in cursor.fetchall():
                prompt_usage.append(dict(row))
            
            # Calculate time taken if job is completed
            time_taken = None
            if job.get('completed_at') and job.get('created_at'):
                time_taken = job['completed_at'] - job['created_at']
            
            metrics = {
                "job_id": job_id,
                "status": job.get('status'),
                "total_urls": job.get('total_urls', 0),
                "processed_urls": job.get('processed_urls', 0),
                "progress": job.get('progress', 0),
                "total_results": result_stats.get('total_results', 0),
                "successful_results": result_stats.get('successful', 0),
                "failed_results": result_stats.get('failed', 0),
                "total_words": result_stats.get('total_words', 0),
                "total_tokens": result_stats.get('total_tokens', 0),
                "prompt_usage": prompt_usage,
                "time_taken": time_taken,
                "time_taken_formatted": self._format_duration(time_taken) if time_taken else None
            }
            
            conn.close()
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting metrics for job {job_id}: {str(e)}")
            return {"error": str(e)}
    
    def export_results_to_csv(self, job_id: str, output_path: Optional[str] = None) -> str:
        """
        Export job results to a CSV file.
        
        Args:
            job_id: The job identifier
            output_path: Optional path for the CSV file
            
        Returns:
            Path to the exported CSV file
        """
        try:
            # Get the job info
            job = self.get_job(job_id)
            
            if not job:
                logger.error(f"Job {job_id} not found for CSV export")
                return ""
            
            # Generate default output path if not provided
            if not output_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                job_name = job.get('name') or job_id
                job_name = ''.join(c if c.isalnum() else '_' for c in job_name)  # Sanitize
                
                output_dir = os.path.join(os.path.dirname(self.db_path), "exports")
                os.makedirs(output_dir, exist_ok=True)
                
                output_path = os.path.join(output_dir, f"{job_name}_{timestamp}.csv")
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get all results for the job
            cursor.execute('SELECT * FROM results WHERE job_id = ?', (job_id,))
            results = cursor.fetchall()
            
            if not results:
                logger.warning(f"No results found for job {job_id}")
                return ""
            
            # First pass: identify all unique fields from the JSON data
            all_fields = set()
            row_dicts = []
            
            for row in results:
                row_dict = dict(row)
                
                # Parse the data JSON
                if 'data' in row_dict and row_dict['data']:
                    try:
                        data = json.loads(row_dict['data'])
                        
                        # Extract fields from the data
                        for key, value in data.items():
                            # Skip metadata and internal fields
                            if key not in ['metadata', 'output', 'token_usage', 'api_requests']:
                                all_fields.add(key)
                        
                        # Merge data fields into row_dict
                        for key, value in data.items():
                            if key not in ['metadata', 'output', 'token_usage', 'api_requests']:
                                row_dict[key] = value
                    except:
                        pass
                
                row_dicts.append(row_dict)
            
            # Define the column order
            standard_fields = [
                'url', 'title', 'status', 'content_type', 'word_count',
                'processed_at_formatted', 'prompt_name', 'api_tokens'
            ]
            
            # Add processed_at_formatted field
            for row_dict in row_dicts:
                if 'processed_at' in row_dict and row_dict['processed_at']:
                    row_dict['processed_at_formatted'] = datetime.fromtimestamp(
                        row_dict['processed_at']).strftime('%Y-%m-%d %H:%M:%S')
            
            # Sort other fields alphabetically
            other_fields = sorted(list(all_fields))
            
            # Final column order
            columns = standard_fields + other_fields
            
            # Write to CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=columns, extrasaction='ignore')
                writer.writeheader()
                
                for row_dict in row_dicts:
                    # Clean up the row dict by removing large fields
                    row_dict.pop('data', None)
                    writer.writerow(row_dict)
            
            conn.close()
            
            logger.info(f"Exported results for job {job_id} to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error exporting results to CSV for job {job_id}: {str(e)}")
            return ""
    
    def export_results_to_excel(self, job_id: str, output_path: Optional[str] = None) -> str:
        """
        Export job results to an Excel file with multiple sheets.
        
        Args:
            job_id: The job identifier
            output_path: Optional path for the Excel file
            
        Returns:
            Path to the exported Excel file
        """
        try:
            # Get the job info
            job = self.get_job(job_id)
            
            if not job:
                logger.error(f"Job {job_id} not found for Excel export")
                return ""
            
            # Generate default output path if not provided
            if not output_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                job_name = job.get('name') or job_id
                job_name = ''.join(c if c.isalnum() else '_' for c in job_name)  # Sanitize
                
                output_dir = os.path.join(os.path.dirname(self.db_path), "exports")
                os.makedirs(output_dir, exist_ok=True)
                
                output_path = os.path.join(output_dir, f"{job_name}_{timestamp}.xlsx")
            
            # Get CSV export as a starting point
            csv_path = self.export_results_to_csv(job_id)
            
            if not csv_path:
                logger.error(f"Failed to create intermediate CSV for Excel export")
                return ""
            
            # Read the CSV and create Excel file using pandas
            df = pd.read_csv(csv_path)
            
            # Create Excel writer
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Main results sheet
                df.to_excel(writer, sheet_name='All Results', index=False)
                
                # Add a summary sheet
                metrics = self.get_job_metrics(job_id)
                
                # Create summary dataframe
                summary_data = {
                    'Metric': [
                        'Job ID', 'Job Name', 'Status', 'Total URLs', 'Processed URLs',
                        'Success Rate', 'Total Words', 'Total API Tokens',
                        'Created At', 'Completed At', 'Time Taken'
                    ],
                    'Value': [
                        job_id,
                        job.get('name', ''),
                        job.get('status', ''),
                        job.get('total_urls', 0),
                        job.get('processed_urls', 0),
                        f"{(metrics.get('successful_results', 0) / max(metrics.get('total_results', 1), 1)) * 100:.1f}%",
                        metrics.get('total_words', 0),
                        metrics.get('total_tokens', 0),
                        job.get('created_at_formatted', ''),
                        job.get('completed_at_formatted', ''),
                        metrics.get('time_taken_formatted', '')
                    ]
                }
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Add prompt usage sheet if available
                if metrics.get('prompt_usage'):
                    prompt_df = pd.DataFrame(metrics['prompt_usage'])
                    prompt_df.to_excel(writer, sheet_name='Prompt Usage', index=False)
                
                # Add company info sheet if available
                company_info = job.get('company_info', {})
                if company_info:
                    company_data = []
                    for key, value in company_info.items():
                        company_data.append({'Field': key, 'Value': value})
                    
                    company_df = pd.DataFrame(company_data)
                    company_df.to_excel(writer, sheet_name='Company Context', index=False)
            
            # Remove the temporary CSV file
            try:
                os.remove(csv_path)
            except:
                pass
                
            logger.info(f"Exported results for job {job_id} to Excel: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error exporting results to Excel for job {job_id}: {str(e)}")
            return ""
    
    def _format_duration(self, seconds: float) -> str:
        """Format a duration in seconds to a human-readable string."""
        if seconds is None:
            return "Unknown"
            
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job and all its associated results.
        
        Args:
            job_id: The job identifier
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Use a transaction to ensure atomicity
            cursor.execute('BEGIN TRANSACTION')
            
            # Delete from prompt_usage
            cursor.execute('DELETE FROM prompt_usage WHERE job_id = ?', (job_id,))
            
            # Delete from results
            cursor.execute('DELETE FROM results WHERE job_id = ?', (job_id,))
            
            # Delete from jobs
            cursor.execute('DELETE FROM jobs WHERE job_id = ?', (job_id,))
            
            cursor.execute('COMMIT')
            conn.close()
            
            logger.info(f"Deleted job {job_id} and associated data")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting job {job_id}: {str(e)}")
            
            # Attempt to rollback transaction
            try:
                cursor.execute('ROLLBACK')
            except:
                pass
                
            return False

    def get_results(self, job_id, limit=100, offset=0):
        """
        Get results for a job with pagination.
        
        Args:
            job_id: The job ID
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            List of result dictionaries
        """
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT * FROM results WHERE job_id = ? ORDER BY processed_at DESC LIMIT ? OFFSET ?", 
                (job_id, int(limit), int(offset))
            )
            
            rows = cursor.fetchall()
            
            # Convert rows to dicts and parse JSON fields
            results = []
            for row in rows:
                result = dict(row)
                # Parse data JSON if present
                if 'data' in result and result['data']:
                    try:
                        result['data'] = json.loads(result['data'])
                    except json.JSONDecodeError:
                        pass
                        
                # Ensure analysis_results exists for template rendering
                if 'analysis_results' not in result:
                    result['analysis_results'] = {}
                    
                results.append(result)
            
            conn.close()
            return results
        except Exception as e:
            logger.error(f"Error in get_results: {str(e)}")
            return []


# Create a global instance for easy import and use throughout the application
db = DatabaseManager()


# Test function to verify the database is working correctly
def test_database():
    """Test the database functionality."""
    try:
        # Create a database in a temporary location
        import tempfile
        test_db_path = os.path.join(tempfile.gettempdir(), "test_analysis.db")
        
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
            
        test_db = DatabaseManager(test_db_path)
        
        # Create a test job
        job_id = "test-job-" + datetime.now().strftime("%Y%m%d%H%M%S")
        company_info = {"name": "ACME Corp", "industry": "Technology"}
        
        test_db.create_job(
            job_id=job_id,
            name="Test Job", 
            prompt_names=["test_prompt"],
            company_info=company_info
        )
        
        # Update job status
        test_db.update_job_status(
            job_id=job_id,
            status="running",
            total_urls=5,
            processed_urls=2
        )
        
        # Add a test result
        test_result = {
            "url": "https://example.com",
            "title": "Example Website",
            "status": "success",
            "content_type": "html",
            "word_count": 1000,
            "prompt_name": "test_prompt",
            "api_tokens": 500,
            "ca_target_audience": "Developers",
            "ca_quality_score": 8,
            "ca_key_themes": "Testing, Databases, Python"
        }
        
        result_id = test_db.save_result(job_id, test_result)
        
        # Get the job
        job = test_db.get_job(job_id)
        
        # Clean up
        test_db.delete_job(job_id)
        os.remove(test_db_path)
        
        print("Database test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in database test: {str(e)}")
        return False
   
if __name__ == "__main__":
    # Run the test if this module is executed directly
    test_database()
