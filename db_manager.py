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
            
            # CRITICAL FIX: Prepare data object with analysis results and structured data
            data_obj = {}
            
            # Include analysis_results if present
            if 'analysis_results' in result:
                data_obj['analysis_results'] = result['analysis_results']
                logger.info(f"Including analysis_results with {len(result['analysis_results'])} prompts")
            
            # Include structured_data if present
            if 'structured_data' in result:
                data_obj['structured_data'] = result['structured_data']
                total_fields = sum(len(fields) for prompt, fields in result['structured_data'].items())
                logger.info(f"Including structured_data with {total_fields} total fields")
            
            # If data is already a serialized string, try to parse and enhance it
            elif 'data' in result and isinstance(result['data'], str) and result['data'] not in ('{}', ''):
                try:
                    existing_data = json.loads(result['data']) 
                    if isinstance(existing_data, dict):
                        data_obj = existing_data
                        logger.info(f"Using existing data with keys: {list(existing_data.keys())}")
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse existing data JSON: {result['data'][:100]}")
            
            # If we have structured fields directly in the result, add them too
            structured_fields = {}
            for key, value in result.items():
                if key.startswith(('ci_', 'pa_', 'ca_')) or ('_' in key and key not in ('job_id', 'api_tokens', 'word_count')):
                    structured_fields[key] = value
            
            # If we found direct structured fields, include them
            if structured_fields:
                if 'structured_data' not in data_obj:
                    data_obj['structured_data'] = {}
                
                # Use the first prompt name or a default
                target_prompt = prompt_name or 'content_analysis'
                if target_prompt not in data_obj['structured_data']:
                    data_obj['structured_data'][target_prompt] = {}
                
                # Add all structured fields
                data_obj['structured_data'][target_prompt].update(structured_fields)
                logger.info(f"Added {len(structured_fields)} direct structured fields to data_obj")

            # Convert the final data object to JSON
            data_json = json.dumps(data_obj)
            
            # Debug log the size of the data we're storing
            logger.info(f"Saving result for {url} with data JSON size: {len(data_json)} chars")
            
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
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
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
    
    
    def get_results_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Get all results for a specific job with structured data properly extracted.
        
        Args:
            job_id: The job ID
            
        Returns:
            List of result dictionaries with parsed data
        """
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM results WHERE job_id = ? ORDER BY processed_at DESC", (job_id,))
            rows = cursor.fetchall()
            
            # Log the first row structure for debugging
            if rows:
                first_row = dict(rows[0])
                logger.info(f"Raw result keys: {list(first_row.keys())}")
                if 'data' in first_row:
                    logger.info(f"Data type: {type(first_row['data'])}")
                    if isinstance(first_row['data'], str):
                        logger.info(f"First 100 chars of data: {first_row['data'][:100]}")
            else:
                logger.warning(f"No results found for job {job_id}")
                return []
            
            # Convert rows to dicts and parse data
            results = []
            for i, row in enumerate(rows):
                result = dict(row)
                
                # Parse data field if present
                if 'data' in result and result['data']:
                    try:
                        # If data is a string, try to parse it as JSON
                        if isinstance(result['data'], str):
                            parsed_data = json.loads(result['data'])
                            result['data'] = parsed_data
                            
                            # Debug only first result to avoid log spam
                            if i == 0:
                                logger.info(f"Parsed data keys: {list(parsed_data.keys()) if isinstance(parsed_data, dict) else 'Not a dict'}")
                        
                        # If parsing succeeds or data was already a dict
                        if isinstance(result['data'], dict):
                            data = result['data']
                            
                            # Extract structured fields directly into result for easy access
                            # From analysis_results
                            if 'analysis_results' in data:
                                for prompt_name, analysis in data['analysis_results'].items():
                                    if isinstance(analysis, dict) and 'parsed_fields' in analysis:
                                        if i == 0:  # Debug only first result
                                            logger.info(f"Found parsed_fields in prompt {prompt_name}: {list(analysis['parsed_fields'].keys())}")
                                        for field_name, field_value in analysis['parsed_fields'].items():
                                            result[field_name] = field_value
                            
                            # From structured_data
                            if 'structured_data' in data:
                                for prompt_name, fields in data['structured_data'].items():
                                    if isinstance(fields, dict):
                                        if i == 0:  # Debug only first result
                                            logger.info(f"Found structured_data in prompt {prompt_name}: {list(fields.keys())}")
                                        for field_name, field_value in fields.items():
                                            result[field_name] = field_value
                    
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"Failed to parse data JSON for URL {result.get('url')}: {str(e)}")
                
                results.append(result)
            
            # Debug summary
            if results:
                fields_found = set()
                for r in results:
                    for k in r.keys():
                        if k not in ['id', 'job_id', 'url', 'title', 'status', 'content_type', 
                                    'word_count', 'processed_at', 'prompt_name', 'api_tokens', 
                                    'error', 'data']:
                            fields_found.add(k)
                
                logger.info(f"Found {len(fields_found)} structured fields in results: {sorted(list(fields_found))}")
            
            conn.close()
            return results
        
        except Exception as e:
            logger.error(f"Error in get_results_for_job: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []
    
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
                        # Format timestamp
                        if field == 'processed_at' and result[field]:
                            try:
                                row[field] = datetime.fromtimestamp(result[field]).strftime('%Y-%m-%d %H:%M:%S')
                            except (ValueError, TypeError):
                                row[field] = result[field]
                        else:
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
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return ""

    def export_results_to_excel(self, job_id):
        """Export job results to Excel with all structured fields, handling double-JSON encoding."""
        try:
            import tempfile, os, json, pandas as pd, logging
            from openpyxl import load_workbook
            from openpyxl.utils import get_column_letter
            
            logger = logging.getLogger("db_manager")
            
            # First export to CSV to reuse the extraction logic
            csv_path = self.export_results_to_csv(job_id)
            
            if not csv_path or not os.path.exists(csv_path):
                logger.error(f"CSV export failed for job {job_id}")
                return ""
                
            # Create a temporary file for Excel
            fd, excel_path = tempfile.mkstemp(suffix='.xlsx')
            os.close(fd)
            
            # Read the CSV into a pandas DataFrame
            df = pd.read_csv(csv_path)
            
            # Categorize fields for different sheets
            standard_fields = ['url', 'title', 'status', 'content_type', 'word_count', 
                             'processed_at', 'prompt_name', 'api_tokens', 'error']
            
            ci_fields = [col for col in df.columns if col.startswith('ci_')]
            pa_fields = [col for col in df.columns if col.startswith('pa_')]
            other_fields = [col for col in df.columns if col not in standard_fields + ci_fields + pa_fields]
            
            # Create Excel writer
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Main sheet with all data
                df.to_excel(writer, sheet_name='All Results', index=False)
                
                # Content Intelligence sheet if CI fields exist
                if ci_fields:
                    ci_df = df[['url', 'title'] + ci_fields]
                    ci_df.to_excel(writer, sheet_name='Content Intelligence', index=False)
                
                # Persona Analysis sheet if PA fields exist
                if pa_fields:
                    pa_df = df[['url', 'title'] + pa_fields]
                    pa_df.to_excel(writer, sheet_name='Persona Analysis', index=False)
                
                # Get job info for summary sheet
                job = self.get_job(job_id)
                metrics = self.get_job_metrics(job_id)
                
                # Create summary sheet
                summary_data = {
                    'Metric': [
                        'Job ID', 'Job Name', 'Status', 'Total URLs', 'Processed URLs',
                        'Success Rate', 'Total Words', 'Total API Tokens',
                        'Created At', 'Completed At'
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
                        job.get('created_at', ''),
                        job.get('completed_at', '')
                    ]
                }
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Adjust column widths after saving
            try:
                wb = load_workbook(excel_path)
                for sheet in wb.worksheets:
                    for col in sheet.columns:
                        max_length = 0
                        column = col[0].column_letter
                        for cell in col:
                            try:
                                if cell.value:
                                    cell_length = len(str(cell.value))
                                    max_length = max(max_length, cell_length)
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)  # Cap at 50
                        sheet.column_dimensions[column].width = adjusted_width
                wb.save(excel_path)
            except Exception as e:
                logger.warning(f"Failed to adjust column widths: {e}")
            
            # Clean up CSV file
            try:
                os.remove(csv_path)
            except:
                pass
            
            logger.info(f"Successfully exported to Excel: {excel_path}")
            return excel_path
        except Exception as e:
            logger.error(f"Error in Excel export: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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
