#!/usr/bin/env python3
"""
Database Manager Module

This module provides a SQLite database interface for storing analysis jobs,
results, and other application data. It handles database initialization,
queries, and data export functionality.
"""

import os
import json
import tempfile
import pandas as pd
import sqlite3
import time
import csv
import logging
import tempfile 
from datetime import datetime
import uuid
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
    Manages database operations for the content analyzer.
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
        self._init_db()
        logger.info(f"Database initialized at {db_path}")
    
    def get_connection(self):
        """
        Get a connection to the SQLite database.
        
        Returns:
            SQLite connection object
        """
        return sqlite3.connect(self.db_path)
    
    def _init_db(self):
        """
        Initialize the database schema if it doesn't exist.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create jobs table with the correct schema
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            completed_at REAL,
            total_urls INTEGER,
            processed_urls INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            prompt_names TEXT,
            company_info TEXT
        )
        ''')
        
        # Create results table with the correct schema
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            url TEXT NOT NULL,
            title TEXT,
            status TEXT NOT NULL,
            content_type TEXT,
            word_count INTEGER,
            processed_at REAL NOT NULL,
            prompt_name TEXT,
            api_tokens INTEGER,
            error TEXT,
            data TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
        ''')
        
        # Create prompt usage table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS prompt_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt_name TEXT NOT NULL,
            job_id TEXT NOT NULL,
            url TEXT NOT NULL,
            tokens_used INTEGER,
            processed_at REAL NOT NULL,
            success BOOLEAN,
            FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_job(self, urls: List[str], prompts: List[str], 
               force_browser: bool = False, premium_proxy: bool = False) -> str:
        """
        Create a new job in the database.
        
        Args:
            urls: List of URLs to process
            prompts: List of prompt names to use
            force_browser: Whether to force browser rendering
            premium_proxy: Whether to use premium proxy
            
        Returns:
            Job ID string
        """
        job_id = str(uuid.uuid4())
        
        job_data = {
            'job_id': job_id,
            'urls': urls,
            'prompts': prompts,
            'status': 'pending',
            'created_at': datetime.now().isoformat(),
            'total_urls': len(urls),
            'processed_urls': 0,
            'force_browser': force_browser,
            'premium_proxy': premium_proxy
        }
        
        # Store the job in the database
        self.db.jobs[job_id] = job_data
        self.write_data()
        
        return job_id

    def update_job_status(self, job_id, status=None, total_urls=None, processed_urls=None, error_count=None, error=None, completed_at=None):
        """
        Update job status and other fields.
        
        Args:
            job_id: Job identifier
            status: New job status
            total_urls: Total URLs in job
            processed_urls: Number of processed URLs
            error_count: Number of errors encountered
            error: Error message (if any)
            completed_at: Completion timestamp
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Build the SQL dynamically based on what's provided
            set_clauses = []
            params = []
            
            if status is not None:
                set_clauses.append("status = ?")
                params.append(status)
            
            if total_urls is not None:
                set_clauses.append("total_urls = ?")
                params.append(total_urls)
            
            if processed_urls is not None:
                set_clauses.append("processed_urls = ?")
                params.append(processed_urls)
            
            if error_count is not None:
                set_clauses.append("error_count = ?")
                params.append(error_count)
            
            # Always update the updated_at timestamp
            set_clauses.append("updated_at = ?")
            params.append(time.time())
            
            if completed_at is not None:
                # If string timestamp provided, use it directly
                if isinstance(completed_at, str):
                    set_clauses.append("completed_at = ?")
                    params.append(completed_at)
                else:
                    # Otherwise use current time
                    set_clauses.append("completed_at = ?")
                    params.append(time.time())
            
            # Only update if we have something to update
            if set_clauses:
                query = f"UPDATE jobs SET {', '.join(set_clauses)} WHERE job_id = ?"
                params.append(job_id)
                
                cursor.execute(query, params)
                conn.commit()
            
            conn.close()
            return True
        
        except sqlite3.Error as e:
            logger.error(f"Database error in update_job_status: {e}")
            return False
    
    def save_result(self, result):
        """
        Save an analysis result to the database.
        
        Args:
            result: Dictionary containing the analysis result
            
        Returns:
            The ID of the saved result
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Extract values from result dictionary with fallbacks
            job_id = result.get('job_id')
            url = result.get('url', '')
            title = result.get('title', '')
            status = result.get('status', 'unknown')
            content_type = result.get('content_type', 'unknown')
            word_count = int(result.get('word_count', 0) or 0)  # Convert None to 0
            api_tokens = int(result.get('api_tokens', 0) or 0)  # Convert None to 0
            error = result.get('error', '')
            prompt_name = result.get('prompt_name', '')
            
            # Format analysis_results or data as JSON
            if 'analysis_results' in result and result['analysis_results']:
                analysis_results = result['analysis_results']
                data_json = json.dumps(analysis_results)
            elif 'data' in result:
                data = result.get('data', {})
                data_json = json.dumps(data) if data else '{}'
            else:
                data_json = '{}'
            
            # Use processed_at instead of created_at for the timestamp
            processed_at = result.get('processed_at', time.time())
            
            # Insert the result
            cursor.execute(
                """
                INSERT INTO results (
                    job_id, url, title, status, content_type, word_count, 
                    processed_at, prompt_name, api_tokens, error, data
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, url, title, status, content_type, word_count, 
                 processed_at, prompt_name, api_tokens, error, data_json)
            )
            
            result_id = cursor.lastrowid
            
            # If there are API tokens used, also log in prompt_usage table
            if api_tokens > 0 and prompt_name:
                try:
                    cursor.execute(
                        """
                        INSERT INTO prompt_usage (
                            prompt_name, job_id, url, tokens_used, processed_at, success
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (prompt_name, job_id, url, api_tokens, processed_at, status == 'success')
                    )
                except Exception as e:
                    logger.warning(f"Failed to insert prompt usage: {e}")
            
            conn.commit()
            conn.close()
            return result_id
            
        except sqlite3.Error as e:
            logger.error(f"Database error in save_result: {e}")
            return None
        except Exception as e:
            logger.error(f"Error saving result: {e}")
            return None
    
    def get_job(self, job_id):
        """
        Get job details by job ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job dictionary or None if not found
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                return None
            
            # Get column names from cursor
            columns = [description[0] for description in cursor.description]
            
            # Create a dictionary from column names and values
            job = dict(zip(columns, row))
            
            # Parse JSON fields
            if 'prompt_names' in job and job['prompt_names']:
                try:
                    job['prompts'] = json.loads(job['prompt_names'])
                except json.JSONDecodeError:
                    job['prompts'] = []
            else:
                job['prompts'] = []
            
            if 'company_info' in job and job['company_info']:
                try:
                    job['company_info'] = json.loads(job['company_info'])
                except json.JSONDecodeError:
                    job['company_info'] = {}
            
            # For consistency with the API, ensure job_id is present
            if 'job_id' not in job and 'id' in job:
                job['job_id'] = job['id']
            
            conn.close()
            return job
            
        except sqlite3.Error as e:
            logger.error(f"Database error in get_job: {e}")
            return None
    

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
    


    def get_results_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Get all results for a specific job.
        
        Args:
            job_id: The job ID
            
        Returns:
            List of result dictionaries
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM results WHERE job_id = ?", (job_id,))
        rows = cursor.fetchall()
        
        # Convert rows to dicts and parse JSON fields
        results = []
        for row in rows:
            result = dict(row)
            # Ensure analysis_results is properly parsed
            if result.get('analysis_results'):
                try:
                    result['analysis_results'] = json.loads(result['analysis_results'])
                except (json.JSONDecodeError, TypeError):
                    result['analysis_results'] = {}
            else:
                result['analysis_results'] = {}
            results.append(result)
        
        conn.close()
        return results
    
    def get_recent_jobs(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get the most recent jobs.
        
        Args:
            limit: Maximum number of jobs to return
            
        Returns:
            List of job dictionaries
        """
        try:
            # Ensure limit is an integer
            try:
                limit = int(limit) if limit is not None else 5
            except (ValueError, TypeError):
                limit = 5
                
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT job_id, name, status, created_at, updated_at, completed_at, " +
                "total_urls, processed_urls, error_count, prompt_names " +
                "FROM jobs ORDER BY created_at DESC LIMIT ?", 
                (limit,)
            )
            rows = cursor.fetchall()
            
            # Get column names from cursor
            columns = [description[0] for description in cursor.description]
            
            jobs = []
            for row in rows:
                job = dict(zip(columns, row))
                
                # Parse JSON fields
                if 'prompt_names' in job and job['prompt_names']:
                    try:
                        job['prompts'] = json.loads(job['prompt_names'])
                    except json.JSONDecodeError:
                        job['prompts'] = []
                else:
                    job['prompts'] = []
                
                # For templates that expect URLs
                job['urls'] = []  
                
                jobs.append(job)
            
            conn.close()
            return jobs
            
        except sqlite3.Error as e:
            logger.error(f"Database error in get_recent_jobs: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_recent_jobs: {e}")
            return []
    
    def get_job_metrics(self, job_id: str) -> Dict[str, Any]:
        """
        Get metrics for a specific job.
        
        Args:
            job_id: The job ID
            
        Returns:
            Metrics dictionary
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get job details
            cursor.execute("SELECT total_urls, processed_urls, error_count FROM jobs WHERE job_id = ?", (job_id,))
            job_row = cursor.fetchone()
            
            if not job_row:
                conn.close()
                return {}
            
            total_urls, processed_urls, error_count = job_row
            
            # Get result metrics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_results,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_results,
                    SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_results,
                    SUM(CASE WHEN status = 'scrape_error' THEN 1 ELSE 0 END) as scrape_errors,
                    SUM(word_count) as total_words,
                    SUM(api_tokens) as total_tokens
                FROM results
                WHERE job_id = ?
            """, (job_id,))
            
            metrics_row = cursor.fetchone()
            conn.close()
            
            if not metrics_row:
                return {
                    'total_urls': total_urls or 0,
                    'processed_urls': processed_urls or 0,
                    'error_count': error_count or 0
                }
            
            total_results, successful_results, failed_results, scrape_errors, total_words, total_tokens = metrics_row
            
            return {
                'total_urls': total_urls or 0,
                'processed_urls': processed_urls or 0,
                'error_count': error_count or 0,
                'total_results': total_results or 0,
                'successful_results': successful_results or 0,
                'failed_results': failed_results or 0,
                'scrape_errors': scrape_errors or 0,
                'total_words': total_words or 0,
                'total_tokens': total_tokens or 0
            }
            
        except sqlite3.Error as e:
            logger.error(f"Database error in get_job_metrics: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error in get_job_metrics: {e}")
            return {}
    
    def delete_job(self, job_id):
        """
        Delete a job and its results.
        
        Args:
            job_id: Job identifier
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Delete job
            cursor.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
            
            # Delete results
            cursor.execute("DELETE FROM results WHERE job_id = ?", (job_id,))
            
            # Delete prompt usage
            cursor.execute("DELETE FROM prompt_usage WHERE job_id = ?", (job_id,))
            
            conn.commit()
            conn.close()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Database error in delete_job: {e}")
            return False

    def get_all_jobs(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with pagination, ordered by creation date (most recent first).
        
        Args:
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip
            
        Returns:
            List of job dictionaries
        """
        try:
            # Ensure limit and offset are integers
            try:
                limit = int(limit) if limit is not None else 100
                offset = int(offset) if offset is not None else 0
            except (ValueError, TypeError):
                logger.warning(f"Invalid parameters: limit={limit}, offset={offset}. Using defaults.")
                limit = 100
                offset = 0
                
            # Ensure values are positive
            limit = max(1, limit)
            offset = max(0, offset)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Use parameterized query with integer values
            cursor.execute(
                """
                SELECT job_id, name, status, created_at, updated_at, completed_at, 
                       total_urls, processed_urls, error_count, prompt_names, company_info
                FROM jobs
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """, 
                (int(limit), int(offset))  # Explicit casting to integers
            )
            
            rows = cursor.fetchall()
            cols = [col[0] for col in cursor.description]
            jobs = []
            
            for row in rows:
                # Create dictionary from row data
                job_data = dict(zip(cols, row))
                
                # Parse JSON fields
                try:
                    prompt_names = json.loads(job_data['prompt_names']) if job_data['prompt_names'] else []
                except (TypeError, json.JSONDecodeError):
                    prompt_names = []
                    
                try:
                    company_info = json.loads(job_data['company_info']) if job_data['company_info'] else {}
                except (TypeError, json.JSONDecodeError):
                    company_info = {}
                
                # Format timestamps
                try:
                    created_at = datetime.fromtimestamp(job_data['created_at']).isoformat() if job_data['created_at'] else None
                except (TypeError, ValueError):
                    created_at = None
                    
                try:
                    updated_at = datetime.fromtimestamp(job_data['updated_at']).isoformat() if job_data['updated_at'] else None
                except (TypeError, ValueError):
                    updated_at = None
                    
                try:
                    completed_at = datetime.fromtimestamp(job_data['completed_at']).isoformat() if job_data['completed_at'] else None
                except (TypeError, ValueError):
                    completed_at = None
                
                # Create formatted job object
                job = {
                    "job_id": job_data['job_id'],
                    "name": job_data['name'] or "",
                    "status": job_data['status'] or "unknown",
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "completed_at": completed_at,
                    "total_urls": job_data['total_urls'] or 0,
                    "processed_urls": job_data['processed_urls'] or 0,
                    "error_count": job_data['error_count'] or 0,
                    "prompts": prompt_names,
                    "prompt_count": len(prompt_names),
                    "company_info": company_info,
                    "progress_percentage": 0
                }
                
                # Calculate progress percentage
                if job["total_urls"] > 0:
                    job["progress_percentage"] = int((job["processed_urls"] / job["total_urls"]) * 100)
                
                # For backward compatibility
                job["url_count"] = job["total_urls"]
                
                jobs.append(job)
            
            conn.close()
            return jobs
                
        except sqlite3.Error as e:
            logger.error(f"Database error in get_all_jobs: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_all_jobs: {str(e)}")
            return []

    def get_job_by_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve job information by job ID.
        
        Args:
            job_id: The job identifier
            
        Returns:
            Job dictionary or None if not found
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT job_id, name, status, created_at, updated_at, completed_at, 
                   total_urls, processed_urls, error_count, prompt_names, company_info
            FROM jobs
            WHERE job_id = ?
            """
            
            cursor.execute(query, (job_id,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                return None
            
            # Parse JSON fields
            prompt_names = json.loads(row[9]) if row[9] and isinstance(row[9], str) else []
            company_info = json.loads(row[10]) if row[10] and isinstance(row[10], str) else {}
            
            # Format timestamps properly
            created_at = datetime.fromtimestamp(row[3]).isoformat() if row[3] else None
            updated_at = datetime.fromtimestamp(row[4]).isoformat() if row[4] else None
            completed_at = datetime.fromtimestamp(row[5]).isoformat() if row[5] else None
            
            job = {
                "job_id": row[0],
                "name": row[1],
                "status": row[2],
                "created_at": created_at,
                "updated_at": updated_at,
                "completed_at": completed_at,
                "total_urls": row[6] or 0,
                "processed_urls": row[7] or 0,
                "error_count": row[8] or 0,
                "prompts": prompt_names,
                "prompt_count": len(prompt_names),
                "company_info": company_info,
                "progress_percentage": 0
            }
            
            # Calculate progress percentage
            if job["total_urls"] > 0:
                job["progress_percentage"] = int((job["processed_urls"] / job["total_urls"]) * 100)
            
            # For backward compatibility
            job["url_count"] = job["total_urls"]
            
            conn.close()
            return job
                
        except sqlite3.Error as e:
            logger.error(f"Database error in get_job_by_id: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in get_job_by_id: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_job_by_id: {str(e)}")
            return None

    def inspect_db_schema(self):
        """
        Inspect the database schema and return information about tables and columns.
        Useful for debugging schema issues.
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            schema_info = {}
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [(col[1], col[2]) for col in cursor.fetchall()]
                schema_info[table] = columns
            
            conn.close()
            return schema_info
        except sqlite3.Error as e:
            logger.error(f"Error inspecting schema: {e}")
            return {}

    def extract_output_fields_from_prompt(self, prompt_name):
        """
        Extract output field definitions from a prompt configuration.
        
        Args:
            prompt_name: Name of the prompt
            
        Returns:
            List of output field dictionaries
        """
        from prompt_loader import get_prompt_by_name
        
        try:
            prompt = get_prompt_by_name(prompt_name)
            if prompt and 'output_fields' in prompt:
                return prompt['output_fields']
        except Exception as e:
            logger.error(f"Error extracting output fields from prompt {prompt_name}: {e}")
        
        return []

    def export_results_to_csv(self, job_id):
        """Export job results to CSV with all structured fields, handling double-JSON encoding."""
        try:
            import tempfile, os, json, csv, logging
            
            logger = logging.getLogger("db_manager")
            
            # Get results for the job
            results = self.get_results_for_job(job_id)
            
            if not results:
                logger.warning(f"No results found for job {job_id}")
                return ""
                
            # Create a temporary file
            fd, csv_path = tempfile.mkstemp(suffix='.csv')
            os.close(fd)
            
            # Collect all fields - start with standard fields
            all_fields = set(['url', 'status', 'title', 'word_count', 'processed_at', 
                             'content_type', 'prompt_name', 'api_tokens', 'error'])
            
            # First pass: collect all field names from the structured data
            for result in results:
                if 'data' in result and result['data']:
                    try:
                        # CRITICAL SECTION: Handle double-JSON encoding
                        data_str = result['data']
                        
                        # Use this approach to handle the double encoding:
                        try:
                            # First try direct parsing - it could be a JSON string or object
                            if isinstance(data_str, str):
                                outer_json = json.loads(data_str)
                            else:
                                outer_json = data_str
                                
                            # If the result is STILL a string, it means double-encoded JSON
                            if isinstance(outer_json, str):
                                data_obj = json.loads(outer_json)
                            else:
                                data_obj = outer_json
                        except json.JSONDecodeError:
                            # If any parsing fails, log and continue
                            logger.error("Failed to parse JSON data")
                            continue
                        
                        # Now extract fields from the analysis text which contains the ||| delimiter
                        if isinstance(data_obj, dict) and 'analysis_results' in data_obj:
                            for prompt_name, analysis in data_obj['analysis_results'].items():
                                if isinstance(analysis, dict) and 'analysis' in analysis:
                                    analysis_text = analysis['analysis']
                                    
                                    # Process the analysis text line by line
                                    for line in analysis_text.split('\n'):
                                        if '|||' in line:
                                            parts = line.split('|||')
                                            if len(parts) == 2:
                                                field_name = parts[0].strip()
                                                all_fields.add(field_name)
                        
                        # Also check structured_data if present
                        if isinstance(data_obj, dict) and 'structured_data' in data_obj:
                            for prompt_name, fields in data_obj['structured_data'].items():
                                if isinstance(fields, dict):
                                    for field_name in fields:
                                        all_fields.add(field_name)
                                        
                        # Also look for parsed_fields
                        if isinstance(data_obj, dict) and 'analysis_results' in data_obj:
                            for prompt_name, analysis in data_obj['analysis_results'].items():
                                if isinstance(analysis, dict) and 'parsed_fields' in analysis:
                                    for field_name in analysis['parsed_fields']:
                                        all_fields.add(field_name)
                    except Exception as e:
                        logger.error(f"Error collecting field names: {str(e)}")
            
            # Log found field count
            logger.info(f"Found {len(all_fields)} total fields to export")
            
            # Second pass: Extract all values
            rows = []
            for result in results:
                # Start with basic fields
                row = {field: '' for field in all_fields}
                
                # Fill in standard fields first
                for field in ['url', 'status', 'title', 'word_count', 'processed_at', 
                             'content_type', 'prompt_name', 'api_tokens', 'error']:
                    if field in result:
                        row[field] = result[field]
                
                # Now extract structured data from the double-encoded JSON
                if 'data' in result and result['data']:
                    try:
                        # Handle double-JSON encoding
                        data_str = result['data']
                        
                        # Parse the double-encoded JSON
                        try:
                            # First parse
                            if isinstance(data_str, str):
                                outer_json = json.loads(data_str)
                            else:
                                outer_json = data_str
                                
                            # Second parse if needed
                            if isinstance(outer_json, str):
                                data_obj = json.loads(outer_json)
                            else:
                                data_obj = outer_json
                        except json.JSONDecodeError:
                            # Skip this result if parsing fails
                            logger.error("Failed to parse JSON data for a result")
                            rows.append(row)
                            continue
                        
                        # Extract fields from analysis_results directly
                        if isinstance(data_obj, dict) and 'analysis_results' in data_obj:
                            for prompt_name, analysis in data_obj['analysis_results'].items():
                                # Handle direct fields in the analysis object
                                if isinstance(analysis, dict):
                                    # Extract from the raw analysis text
                                    if 'analysis' in analysis:
                                        analysis_text = analysis['analysis']
                                        for line in analysis_text.split('\n'):
                                            if '|||' in line:
                                                parts = line.split('|||')
                                                if len(parts) == 2:
                                                    field_name = parts[0].strip()
                                                    field_value = parts[1].strip()
                                                    row[field_name] = field_value
                                    
                                    # Also check parsed_fields
                                    if 'parsed_fields' in analysis and isinstance(analysis['parsed_fields'], dict):
                                        for field_name, value in analysis['parsed_fields'].items():
                                            if isinstance(value, list):
                                                row[field_name] = ', '.join(str(v) for v in value if v is not None)
                                            else:
                                                row[field_name] = value
                        
                        # Also check structured_data if present
                        if isinstance(data_obj, dict) and 'structured_data' in data_obj:
                            for prompt_name, fields in data_obj['structured_data'].items():
                                if isinstance(fields, dict):
                                    for field_name, value in fields.items():
                                        if isinstance(value, list):
                                            row[field_name] = ', '.join(str(v) for v in value if v is not None)
                                        else:
                                            row[field_name] = value
                                        
                    except Exception as e:
                        logger.error(f"Error extracting data values: {str(e)}")
                
                # Add the row to our results
                rows.append(row)
            
            # Write to CSV file
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_fields))
                writer.writeheader()
                writer.writerows(rows)
            
            logger.info(f"Successfully exported to CSV: {csv_path}")
            return csv_path
        except Exception as e:
            logger.error(f"Error in CSV export: {str(e)}")
            return ""


    def export_results_to_excel(self, job_id):
        """Export job results to Excel with all structured fields, handling double-JSON encoding."""
        try:
            import tempfile, os, json, pandas as pd, logging
            
            logger = logging.getLogger("db_manager")
            
            # Get results for the job
            results = self.get_results_for_job(job_id)
            
            if not results:
                logger.warning(f"No results found for job {job_id}")
                return ""
                
            # Create a temporary file
            fd, excel_path = tempfile.mkstemp(suffix='.xlsx')
            os.close(fd)
            
            # Collect all fields - start with standard fields
            all_fields = set(['url', 'status', 'title', 'word_count', 'processed_at', 
                             'content_type', 'prompt_name', 'api_tokens', 'error'])
            
            # First pass: collect all field names from the structured data
            for result in results:
                if 'data' in result and result['data']:
                    try:
                        # CRITICAL SECTION: Handle double-JSON encoding
                        data_str = result['data']
                        
                        # Use this approach to handle the double encoding:
                        try:
                            # First try direct parsing - it could be a JSON string or object
                            if isinstance(data_str, str):
                                outer_json = json.loads(data_str)
                            else:
                                outer_json = data_str
                                
                            # If the result is STILL a string, it means double-encoded JSON
                            if isinstance(outer_json, str):
                                data_obj = json.loads(outer_json)
                            else:
                                data_obj = outer_json
                        except json.JSONDecodeError:
                            # If any parsing fails, log and continue
                            logger.error("Failed to parse JSON data")
                            continue
                        
                        # Now extract fields from the analysis text which contains the ||| delimiter
                        if isinstance(data_obj, dict) and 'analysis_results' in data_obj:
                            for prompt_name, analysis in data_obj['analysis_results'].items():
                                if isinstance(analysis, dict) and 'analysis' in analysis:
                                    analysis_text = analysis['analysis']
                                    
                                    # Process the analysis text line by line
                                    for line in analysis_text.split('\n'):
                                        if '|||' in line:
                                            parts = line.split('|||')
                                            if len(parts) == 2:
                                                field_name = parts[0].strip()
                                                all_fields.add(field_name)
                        
                        # Also check structured_data if present
                        if isinstance(data_obj, dict) and 'structured_data' in data_obj:
                            for prompt_name, fields in data_obj['structured_data'].items():
                                if isinstance(fields, dict):
                                    for field_name in fields:
                                        all_fields.add(field_name)
                                        
                        # Also look for parsed_fields
                        if isinstance(data_obj, dict) and 'analysis_results' in data_obj:
                            for prompt_name, analysis in data_obj['analysis_results'].items():
                                if isinstance(analysis, dict) and 'parsed_fields' in analysis:
                                    for field_name in analysis['parsed_fields']:
                                        all_fields.add(field_name)
                    except Exception as e:
                        logger.error(f"Error collecting field names: {str(e)}")
            
            # Log found field count
            logger.info(f"Found {len(all_fields)} total fields to export")
            
            # Second pass: Extract all values
            rows = []
            for result in results:
                # Start with basic fields
                row = {field: '' for field in all_fields}
                
                # Fill in standard fields first
                for field in ['url', 'status', 'title', 'word_count', 'processed_at', 
                             'content_type', 'prompt_name', 'api_tokens', 'error']:
                    if field in result:
                        row[field] = result[field]
                
                # Now extract structured data from the double-encoded JSON
                if 'data' in result and result['data']:
                    try:
                        # Handle double-JSON encoding
                        data_str = result['data']
                        
                        # Parse the double-encoded JSON
                        try:
                            # First parse
                            if isinstance(data_str, str):
                                outer_json = json.loads(data_str)
                            else:
                                outer_json = data_str
                                
                            # Second parse if needed
                            if isinstance(outer_json, str):
                                data_obj = json.loads(outer_json)
                            else:
                                data_obj = outer_json
                        except json.JSONDecodeError:
                            # Skip this result if parsing fails
                            logger.error("Failed to parse JSON data for a result")
                            rows.append(row)
                            continue
                        
                        # Extract fields from analysis_results directly
                        if isinstance(data_obj, dict) and 'analysis_results' in data_obj:
                            for prompt_name, analysis in data_obj['analysis_results'].items():
                                # Handle direct fields in the analysis object
                                if isinstance(analysis, dict):
                                    # Extract from the raw analysis text
                                    if 'analysis' in analysis:
                                        analysis_text = analysis['analysis']
                                        for line in analysis_text.split('\n'):
                                            if '|||' in line:
                                                parts = line.split('|||')
                                                if len(parts) == 2:
                                                    field_name = parts[0].strip()
                                                    field_value = parts[1].strip()
                                                    row[field_name] = field_value
                                    
                                    # Also check parsed_fields
                                    if 'parsed_fields' in analysis and isinstance(analysis['parsed_fields'], dict):
                                        for field_name, value in analysis['parsed_fields'].items():
                                            if isinstance(value, list):
                                                row[field_name] = ', '.join(str(v) for v in value if v is not None)
                                            else:
                                                row[field_name] = value
                        
                        # Also check structured_data if present
                        if isinstance(data_obj, dict) and 'structured_data' in data_obj:
                            for prompt_name, fields in data_obj['structured_data'].items():
                                if isinstance(fields, dict):
                                    for field_name, value in fields.items():
                                        if isinstance(value, list):
                                            row[field_name] = ', '.join(str(v) for v in value if v is not None)
                                        else:
                                            row[field_name] = value
                                        
                    except Exception as e:
                        logger.error(f"Error extracting data values: {str(e)}")
                
                # Add the row to our results
                rows.append(row)
            
            # Create DataFrame and export to Excel
            df = pd.DataFrame(rows)
            df.to_excel(excel_path, index=False, engine='openpyxl')
            
            logger.info(f"Successfully exported to Excel: {excel_path}")
            return excel_path
        except Exception as e:
            logger.error(f"Error in Excel export: {str(e)}")
            return ""
        
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
        job_id = test_db.create_job(
            urls=["https://example.com"],
            prompts=["test_prompt"],
            name="Test Job",
            company_info={"name": "ACME Corp", "industry": "Technology"}
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
            "job_id": job_id,
            "url": "https://example.com",
            "title": "Example Website",
            "status": "success",
            "content_type": "html",
            "word_count": 1000,
            "api_tokens": 500,
            "processed_at": time.time(),
            "prompt_name": "test_prompt",
            "data": {
                "test_prompt": {
                    "ca_target_audience": "Developers",
                    "ca_quality_score": 8,
                    "ca_key_themes": "Testing, Databases, Python"
                }
            }
        }
        
        test_db.save_result(test_result)
        
        # Get the job
        job = test_db.get_job(job_id)
        
        # Get the results
        results = test_db.get_results_for_job(job_id)

        # Update job to completed
        test_db.update_job_status(
            job_id=job_id,
            status="completed",
            processed_urls=5
        )

        # Get metrics
        metrics = test_db.get_job_metrics(job_id)

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
