#!/usr/bin/env python3
"""
Content Analysis Flask Application

This is the main Flask application that provides a web interface
for content analysis using OpenAI API and custom prompt configurations.
"""


import pytz
import asyncio
import os
import json
import time
import uuid
import logging
import threading
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from flask import Flask, request, jsonify, render_template, redirect, url_for, send_file, Response, flash
# Import our custom modules
from db_manager import db
from analyzer import ContentAnalyzer
from prompt_loader import list_available_prompts, get_prompt_by_name, PromptLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("app")

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", os.urandom(24).hex())
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload size

# Create a global analyzer instance
analyzer = ContentAnalyzer()

# Set up job queue for background processing
job_queue = {}
active_threads = set()


def detect_content_type(url: str, default_type: str = "auto") -> str:
    """
    Detect content type from URL extension or return default.
    
    Args:
        url: URL to analyze
        default_type: Default content type to return if no extension matches
        
    Returns:
        Detected content type (html, pdf, docx, pptx)
    """
    url_lower = url.lower()
    
    # Check extensions
    if url_lower.endswith(".pdf"):
        return "pdf"
    elif url_lower.endswith(".docx"):
        return "docx"
    elif url_lower.endswith(".pptx"):
        return "pptx"
    elif url_lower.endswith((".html", ".htm", ".php", ".asp", ".aspx")):
        return "html"
        
    # Check default type
    if default_type != "auto":
        return default_type
        
    # Default to HTML for any other URL
    return "html"



def process_urls_in_parallel(job_id: str, url_data_list: List[Dict[str, Any]], 
                           prompt_names: List[str], force_browser: bool = False, 
                           premium_proxy: bool = False):
    """
    Process URLs in parallel with direct ContentAnalyzer usage.
    
    Args:
        job_id: The job identifier
        url_data_list: List of URL data dictionaries or URL strings
        prompt_names: List of prompt configuration names to use
        force_browser: Whether to force browser rendering for all URLs
        premium_proxy: Whether to use premium proxy for all URLs
    """
    try:
        logger.info(f"Starting parallel processing for job {job_id} with {len(url_data_list)} URLs")
        
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Initialize counters
        total_urls = len(url_data_list)
        processed_count = 0
        error_count = 0
        
        # Update job status to running
        db.update_job_status(
            job_id=job_id,
            status="running",
            total_urls=total_urls,
            processed_urls=0,
            error_count=0
        )
        
        # Create analyzer instance
        analyzer_instance = ContentAnalyzer()
        
        # Define async processing function
        async def process_urls():
            nonlocal processed_count, error_count
            max_concurrency = int(os.environ.get("MAX_CONCURRENCY", "3"))
            semaphore = asyncio.Semaphore(max_concurrency)
            
            async def process_one_url(url_data):
                async with semaphore:
                    # Extract URL info
                    url = url_data.get('url') if isinstance(url_data, dict) else url_data
                    company_info = url_data.get('company_info') if isinstance(url_data, dict) else None
                    content_type = url_data.get('content_type', 'html') if isinstance(url_data, dict) else 'html'
                    use_browser = url_data.get('force_browser', force_browser) if isinstance(url_data, dict) else force_browser
                    
                    try:
                        # Process URL
                        result = await analyzer_instance.process_url_async(
                            url=url,
                            prompt_names=prompt_names,
                            company_info=company_info,
                            content_type=content_type,
                            force_browser=use_browser
                        )
                        
                        # Add job ID
                        result['job_id'] = job_id
                        
                        # Prepare for database storage
                        prompt_name = prompt_names[0] if prompt_names else ""
                        processed_at = time.time()
                        
                        # Format result for database
                        db_result = {
                            'job_id': job_id,
                            'url': url,
                            'status': result.get('status', 'unknown'),
                            'title': result.get('title', ''),
                            'content_type': result.get('content_type', 'html'),
                            'word_count': result.get('word_count', 0),
                            'processed_at': processed_at,
                            'prompt_name': prompt_name,
                            'api_tokens': result.get('api_tokens', 0),
                            'error': result.get('error', ''),
                            'analysis_results': result.get('analysis_results', {}),
                            'structured_data': result.get('structured_data', {})
                        }
                        
                        # Create a data object combining analysis results and structured data
                        data_obj = {
                            'analysis_results': result.get('analysis_results', {}),
                            'structured_data': result.get('structured_data', {})
                        }
                        
                        # Convert to JSON for storage
                        db_result['data'] = json.dumps(data_obj)
                        
                        # Also add structured fields directly to result for easy access
                        if 'structured_data' in result and prompt_name in result['structured_data']:
                            structured_fields = result['structured_data'][prompt_name]
                            for field_name, field_value in structured_fields.items():
                                db_result[field_name] = field_value
                        
                        return db_result
                    except Exception as e:
                        logger.error(f"Error processing URL {url}: {str(e)}")
                        return {
                            "url": url,
                            "job_id": job_id,
                            "status": "error",
                            "error": str(e),
                            "processed_at": time.time()
                        }
            
            # Create tasks
            tasks = []
            for url_item in url_data_list:
                if isinstance(url_item, str):
                    tasks.append(process_one_url({'url': url_item}))
                else:
                    tasks.append(process_one_url(url_item))
            
            # Run all tasks
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Exception: {str(result)}")
                    error_count += 1
                    continue
                
                # Save to database
                try:
                    db.save_result(result)
                    processed_count += 1
                    if result.get('status') != 'success':
                        error_count += 1
                        
                    # Update job status
                    db.update_job_status(
                        job_id=job_id,
                        processed_urls=processed_count,
                        error_count=error_count
                    )
                except Exception as e:
                    logger.error(f"Error saving result: {str(e)}")
                    error_count += 1
            
            # Determine final status
            final_status = "completed"
            if processed_count == 0:
                final_status = "failed"
            elif error_count > 0:
                final_status = "completed_with_errors"
                
            # Update final job status
            db.update_job_status(
                job_id=job_id,
                status=final_status,
                processed_urls=processed_count,
                error_count=error_count,
                completed_at=time.time()
            )
            
            return processed_count, error_count
        
        # Run the async processing
        processed, errors = loop.run_until_complete(process_urls())
        logger.info(f"Job {job_id} completed: {processed}/{total_urls} URLs processed, {errors} errors")
        return True
        
    except Exception as e:
        logger.error(f"Error in parallel processing: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Mark job as failed
        db.update_job_status(job_id=job_id, status="failed", error_count=1) 
        return False
        
    finally:
        # Cleanup
        if 'loop' in locals():
            loop.close()
            
        # Remove thread from active threads
        if threading.current_thread().ident in active_threads:
            active_threads.remove(threading.current_thread().ident)




def process_urls_in_background(job_id, url_data_list, prompt_names, company_info=None):
    """
    Process URLs in background thread using concurrency.
    """
    try:
        import concurrent.futures
        from concurrent.futures import ThreadPoolExecutor
        
        logger.info(f"Starting background processing for job {job_id} with {len(url_data_list)} URLs")
        
        # Verify job_id is present
        if not job_id:
            logger.error("Missing job_id in background processing")
            return False
        
        # Initialize counters
        processed_count = 0
        error_count = 0
        
        # Update job status to running
        db.update_job_status(
            job_id=job_id,
            status="running",
            total_urls=len(url_data_list),
            processed_urls=0,
            error_count=0
        )
        
        # Initialize analyzer
        analyzer = ContentAnalyzer()
        
        # Determine max concurrency - use environment variable or default to 20
        max_workers = min(int(os.environ.get('MAX_CONCURRENCY', '20')), len(url_data_list))
        logger.info(f"Processing with max concurrency of {max_workers} workers")
        
        # Define function to process a single URL
        def process_single_url(url_data):
            try:
                # Get URL from data
                url = url_data.get('url') if isinstance(url_data, dict) else url_data
                
                # Process URL
                result = analyzer.process_url(
                    url=url,
                    prompt_names=prompt_names,
                    company_info=company_info
                )
                
                # Ensure job_id is set
                result['job_id'] = job_id
                
                return result, None  # Return result and no error
            except Exception as e:
                logger.error(f"Error processing URL {url}: {str(e)}")
                return None, str(e)  # Return no result and the error
        
        # Use ThreadPoolExecutor to process URLs concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all URLs for processing
            future_to_url = {executor.submit(process_single_url, url_data): url_data 
                            for url_data in url_data_list}
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_url):
                url_data = future_to_url[future]
                url = url_data.get('url') if isinstance(url_data, dict) else url_data
                
                try:
                    result, error = future.result()
                    
                    # Update counters
                    processed_count += 1
                    
                    if error:
                        error_count += 1
                        logger.error(f"Error processing URL {url}: {error}")
                    else:
                        # Save result to database
                        db.save_result(result)
                        
                        # Check if result indicates an error
                        if result.get('status') != 'success':
                            error_count += 1
                    
                    # Update job status
                    db.update_job_status(
                        job_id=job_id,
                        status="running",
                        processed_urls=processed_count,
                        error_count=error_count
                    )
                    
                    logger.info(f"Processed URL {processed_count}/{len(url_data_list)} for job {job_id}")
                    
                except Exception as e:
                    processed_count += 1
                    error_count += 1
                    logger.error(f"Error handling result for URL {url}: {str(e)}")
                    
                    # Update job status
                    db.update_job_status(
                        job_id=job_id,
                        status="running",
                        processed_urls=processed_count,
                        error_count=error_count
                    )
        
        # Update job status to completed
        final_status = "completed"
        if processed_count == 0:
            final_status = "failed"
        elif error_count > 0:
            final_status = "completed_with_errors"
        
        db.update_job_status(
            job_id=job_id,
            status=final_status,
            processed_urls=processed_count,
            error_count=error_count,
            completed_at=time.time()
        )
        
        logger.info(f"Completed background processing for job {job_id} - status: {final_status}")
        return True
        
    except Exception as e:
        logger.error(f"Error in background processing: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Update job status to failed
        try:
            db.update_job_status(
                job_id=job_id,
                status="failed",
                error_count=error_count if 'error_count' in locals() else 1
            )
        except:
            pass
        
        return False
    
    finally:
        # Remove thread from active threads
        if threading.current_thread().ident in active_threads:
            active_threads.remove(threading.current_thread().ident)

@app.template_filter('format_date')
def format_date(value, format='%Y-%m-%d %H:%M:%S'):
    """Format a date time to a readable format."""
    if value is None:
        return ""
    
    try:
        # Handle float timestamps (UNIX timestamps)
        if isinstance(value, (float, int)):
            dt = datetime.fromtimestamp(value)
            return dt.strftime(format)
        
        # Handle ISO format strings
        if isinstance(value, str):
            try:
                # Try to parse ISO format
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return dt.strftime(format)
            except ValueError:
                # If parse fails, return as is
                return value
        
        # Handle datetime objects directly
        if hasattr(value, 'strftime'):
            return value.strftime(format)
        
        # Default case
        return str(value)
    except Exception as e:
        # Safely handle any unexpected errors
        logger.warning(f"Date format error: {str(e)}")
        return str(value)



@app.template_filter('get')
def get_attribute(obj, attr, default=""):
    """Safely get an attribute or key from an object/dict."""
    if obj is None:
        return default
    
    # Try dict-like access
    if isinstance(obj, dict):
        return obj.get(attr, default)
    
    # Try attribute access
    if hasattr(obj, attr):
        return getattr(obj, attr)
    
    # Default
    return default


# Flask routes

@app.route('/debug/schema')
def debug_schema():
    """Debug endpoint to view database schema"""
    schema = db.inspect_db_schema()
    return render_template('debug_schema.html', schema=schema)


@app.route('/')
def index():
    """Render the home page."""
    try:
        # Fetch recent jobs with error handling
        recent_jobs = []
        try:
            recent_jobs = db.get_all_jobs(limit=10)
        except Exception as e:
            logger.error(f"Error fetching jobs: {str(e)}")
        
        # Get prompt configurations
        available_prompts = []
        try:
            prompt_loader = PromptLoader()
            available_prompts = prompt_loader.get_prompt_configs()
        except Exception as e:
            logger.error(f"Error loading prompts: {str(e)}")
        
        # Pass all required data to template with defaults for missing data
        return render_template(
            'index.html',
            jobs=recent_jobs or [],
            prompts=available_prompts or [],
            job_count=len(recent_jobs),
            page_title="Content Analyzer Dashboard",
            current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    except Exception as e:
        logger.error(f"Index page error: {str(e)}")
        # Return a simple error page if everything fails
        return render_template(
            'error.html', 
            error=f"Error loading dashboard: {str(e)}",
            page_title="Error"
        )

def parse_csv_file(file):
    """Parse a CSV file for URLs with company context."""
    import csv
    from io import StringIO
    import re
    import logging
    
    logger = logging.getLogger("app")
    
    url_data_list = []
    content = file.read().decode('utf-8-sig')  # Use utf-8-sig to handle BOM character
    
    # Log the first few characters to check for BOM
    logger.info(f"CSV first 20 chars: {repr(content[:20])}")
    
    # Get reader
    reader = csv.DictReader(StringIO(content))
    
    # Find the URL column - handle BOM character
    url_column = None
    for header in reader.fieldnames or []:
        if header is None:
            continue
        if header.strip('\ufeff').lower() == 'url':  # Strip BOM character and case-insensitive
            url_column = header
            break
    
    if not url_column:
        logger.error(f"No URL column found in CSV. Headers: {reader.fieldnames}")
        return []
    
    # Reset reader
    reader = csv.DictReader(StringIO(content))
    
    # Process rows
    row_count = 0
    for row in reader:
        row_count += 1
        
        # Get URL using the correct column name (with or without BOM)
        if url_column in row and row[url_column] and row[url_column].strip():
            url = row[url_column].strip()
            
            # Create URL data with basic fields
            url_data = {'url': url}
            
            # Extract company info if available
            company_info = {}
            
            # Map CSV fields to expected fields, handling potential BOM
            field_pairs = [
                ('company_name', 'name'),
                ('company_description', 'description'),
                ('company_industry', 'industry'), 
                ('company_revenue', 'revenue')
            ]
            
            for csv_field, internal_field in field_pairs:
                # Look for the field name with potential BOM
                actual_field = None
                for header in row.keys():
                    if header and header.strip('\ufeff').lower() == csv_field.lower():
                        actual_field = header
                        break
                
                if actual_field and row[actual_field]:
                    company_info[internal_field] = row[actual_field]
            
            # Handle industry field that may be in list format as string
            if 'industry' in company_info and isinstance(company_info['industry'], str):
                if company_info['industry'].startswith('[') and company_info['industry'].endswith(']'):
                    try:
                        # Extract from list format string
                        industry_str = company_info['industry'][1:-1]
                        industries = re.findall(r"'([^']*)'|\"([^\"]*)\"", industry_str)
                        company_info['industry'] = [i[0] or i[1] for i in industries if i[0] or i[1]]
                    except Exception as e:
                        logger.warning(f"Error parsing industry list: {str(e)}")
            
            # Add company info if any fields were found
            if company_info:
                url_data['company_info'] = company_info
            
            # Add content type if available
            content_type_col = None
            for header in row.keys():
                if header and header.strip('\ufeff').lower() == 'content_type':
                    content_type_col = header
                    break
                    
            if content_type_col and row[content_type_col]:
                url_data['content_type'] = row[content_type_col]
            
            # Add browser flag if available
            force_browser_col = None
            for header in row.keys():
                if header and header.strip('\ufeff').lower() == 'force_browser':
                    force_browser_col = header
                    break
                    
            if force_browser_col:
                url_data['force_browser'] = str(row[force_browser_col]).lower() in ('true', 'yes', '1')
            
            url_data_list.append(url_data)
    
    logger.info(f"Parsed {row_count} rows from CSV with BOM-aware parsing, found {len(url_data_list)} URLs")
    return url_data_list


def get_status_description(status):
    """Get a human-readable description for a job status."""
    descriptions = {
        'pending': 'Job is waiting to be processed.',
        'running': 'Job is currently being processed.',
        'completed': 'Job completed successfully.',
        'completed_with_errors': 'Job completed but encountered some errors.',
        'failed': 'Job failed to complete.',
        'cancelled': 'Job was cancelled.'
    }
    return descriptions.get(status, f"Status: {status}")



@app.route('/analyze', methods=['POST'])
def analyze():
    """
    Handle form submission for URL analysis.
    """
    try:
        # Get form data
        urls_input = request.form.get('urls', '').strip()
        prompt_names = request.form.getlist('prompts')
        default_content_type = request.form.get('default_content_type', 'auto')
        
        # Initialize url_data_list
        url_data_list = []
        
        # Check input file first (prioritize file over text input)
        if 'file' in request.files and request.files['file'].filename:
            file = request.files['file']
            
            # Check file extension
            if file.filename.endswith('.csv'):
                # Parse CSV file for URLs with company context
                url_data_list = parse_csv_file(file)
            elif file.filename.endswith(('.xlsx', '.xls')):
                # Parse Excel file for URLs with company context
                if 'parse_excel_file' in globals():
                    url_data_list = parse_excel_file(file)
                else:
                    flash('Excel parsing is not implemented', 'warning')
            elif file.filename.endswith('.json'):
                # Parse JSON file for URLs with company context
                if 'parse_json_file' in globals():
                    url_data_list = parse_json_file(file)
                else:
                    flash('JSON parsing is not implemented', 'warning')
            else:
                # Assume text file with one URL per line (no company context)
                content = file.read().decode('utf-8')
                urls = [line.strip() for line in content.split('\n') if line.strip()]
                url_data_list = []
                
                # Apply content type detection to each URL
                for url in urls:
                    url_data_list.append({
                        'url': url,
                        'content_type': detect_content_type(url, default_content_type)
                    })
        else:
            # Parse URLs from text input
            urls = [line.strip() for line in urls_input.split('\n') if line.strip()]
            url_data_list = []
            
            # Apply content type detection to each URL
            for url in urls:
                url_data_list.append({
                    'url': url,
                    'content_type': detect_content_type(url, default_content_type)
                })
        
        # Filter out invalid URLs and ensure content types
        valid_url_data_list = []
        for data in url_data_list:
            if is_valid_url(data.get('url', '')):
                # Make sure every URL has a content_type
                if 'content_type' not in data or not data['content_type']:
                    data['content_type'] = detect_content_type(data['url'], default_content_type)
                valid_url_data_list.append(data)
        
        if not valid_url_data_list:
            flash('No valid URLs provided.', 'danger')
            return redirect(url_for('index'))
        
        if not prompt_names:
            flash('Please select at least one prompt configuration.', 'warning')
            return redirect(url_for('index'))
        
        # Get company info (if provided)
        company_info = request.form.get('company_info', None)
        if company_info:
            try:
                company_info = json.loads(company_info)
            except:
                company_info = None
        
        # Create a new job
        job_id = db.create_job(
            urls=[data.get('url') for data in valid_url_data_list],
            prompts=prompt_names,
            name=f"Analysis {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        
        # CRITICAL FIX: Verify job_id was created properly
        if not job_id:
            logger.error("Failed to create job - no job_id returned")
            flash("Failed to create analysis job", 'danger')
            return redirect(url_for('index'))
        
        logger.info(f"Created job with ID: {job_id}")
        
        # Start processing in a background thread
        thread = threading.Thread(
            target=process_urls_in_background,
            args=(job_id, valid_url_data_list, prompt_names, company_info)
        )
        thread.daemon = True
        thread.start()
        
        # Add thread ID to active threads
        active_threads.add(thread.ident)
        
        # Redirect to job status page with job_id
        return redirect(url_for('job_status', job_id=job_id))
            
    except Exception as e:
        logger.error(f"Error starting analysis: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        flash(f"Error: {str(e)}", 'danger')
        return redirect(url_for('index'))


@app.route('/job/<string:job_id>')
def job_status(job_id):
    """
    Display job status page.
    """
    try:
        job = db.get_job(job_id)
        
        if not job:
            return render_template('error.html', error=f"Job {job_id} not found"), 404
        
        results = db.get_results_for_job(job_id)
        
        # Ensure each result has analysis_results
        for result in results:
            if not result.get('analysis_results'):
                result['analysis_results'] = {}
        
        # Initialize metrics with default values
        metrics = {
            'total_urls': job.get('total_urls', 0),
            'processed_urls': job.get('processed_urls', 0),
            'successful_results': 0,
            'failed_results': 0,
            'scrape_errors': 0,
            'total_tokens': 0,
            'total_words': 0
        }
        
        # Calculate metrics only if we have results
        if results:
            successful = [r for r in results if r.get('status') == 'success']
            failed = [r for r in results if r.get('status') != 'success']
            scrape_errors = [r for r in results if r.get('status') == 'scrape_error']
            
            metrics.update({
                'successful_results': len(successful),
                'failed_results': len(failed),
                'scrape_errors': len(scrape_errors),
                'total_tokens': sum(r.get('api_tokens', 0) for r in results),
                'total_words': sum(r.get('word_count', 0) for r in results)
            })
        
        return render_template(
            'job.html', 
            job=job, 
            results=results, 
            metrics=metrics,
            status_description=get_status_description(job.get('status', ''))
        )
    except Exception as e:
        logger.error(f"Error displaying job status: {str(e)}")
        return render_template('error.html', error=f"Error loading job: {str(e)}"), 500

@app.template_filter('safe_round')
def safe_round(value, precision=0):
    """Safely round a value, handling None and undefined values."""
    try:
        if value is None:
            return 0
        return round(float(value), precision)
    except (TypeError, ValueError):
        return 0


@app.route('/api/job/<job_id>')
def api_job_status(job_id):
    """Get job status as JSON."""
    # Get job details
    job = db.get_job(job_id)
    
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    # Get metrics
    metrics = db.get_job_metrics(job_id)
    
    # If job status is "pending" but processing has started, update to "running"
    if job.get('status') == 'pending' and job.get('processed_urls', 0) > 0:
        db.update_job_status(
            job_id=job_id,
            status='running'
        )
        job['status'] = 'running'
    
    # If job status is "running" but all URLs have been processed, update to "completed"
    if job.get('status') == 'running' and job.get('processed_urls') == job.get('total_urls'):
        final_status = 'completed'
        if metrics.get('failed_results', 0) > 0:
            final_status = 'completed_with_errors'
            
        db.update_job_status(
            job_id=job_id,
            status=final_status,
            completed_at=datetime.now().isoformat()
        )
        job['status'] = final_status
    
    return jsonify({
        'job': job,
        'metrics': metrics
    })

@app.route('/api/job/<job_id>/results')
def api_job_results(job_id):
    """Get job results as JSON."""
    # Get job details
    job = db.get_job(job_id)
    
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    # Get results (limited by query parameter)
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('offset', 0))
    
    results = db.get_results(job_id, limit=limit, offset=offset)
    
    return jsonify({
        'job_id': job_id,
        'count': len(results),
        'results': results
    })

@app.route('/job/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """Cancel a running job."""
    # Get job details
    job = db.get_job(job_id)
    
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    # Can only cancel running jobs
    if job.get('status') != 'running':
        return jsonify({'error': 'Job is not running'}), 400
    
    # Update job status to cancelled
    db.update_job_status(job_id, status='cancelled')
    
    return redirect(url_for('job_status', job_id=job_id))

@app.route('/job/<job_id>/delete', methods=['POST'])
def delete_job(job_id):
    """Delete a job."""
    # Get job details
    job = db.get_job(job_id)
    
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    # Cannot delete running jobs
    if job.get('status') == 'running':
        return jsonify({'error': 'Cannot delete a running job'}), 400
    
    # Delete the job
    db.delete_job(job_id)
    
    return redirect(url_for('index'))

@app.route('/job/<job_id>/export')
def export_job(job_id):
    """Export job results."""
    # Get job details
    job = db.get_job(job_id)
    
    if not job:
        return render_template('error.html', error="Job not found"), 404
    
    # Get export format
    export_format = request.args.get('format', 'csv')
    
    if export_format == 'csv':
        # Export to CSV
        csv_path = db.export_results_to_csv(job_id)
        
        if not csv_path:
            return render_template('error.html', error="Export failed"), 500
        
        return send_file(
            csv_path,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"results_{job_id}.csv"
        )
    elif export_format == 'excel':
        # Export to Excel
        excel_path = db.export_results_to_excel(job_id)
        
        if not excel_path:
            return render_template('error.html', error="Export failed"), 500
        
        return send_file(
            excel_path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"results_{job_id}.xlsx"
        )
    else:
        return render_template('error.html', error="Invalid export format"), 400

@app.route('/prompts')
def list_prompts():
    """List all available prompts."""
    # Get available prompts
    prompts = list_available_prompts()
    
    return render_template(
        'prompts.html',
        prompts=prompts,
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )

@app.route('/prompts/<name>')
def view_prompt(name):
    """View a specific prompt."""
    # Get the prompt
    prompt = get_prompt_by_name(name)
    
    if not prompt:
        return render_template('error.html', error="Prompt not found"), 404
    
    return render_template(
        'prompt_detail.html',
        prompt=prompt,
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )

@app.route('/jobs')
def list_jobs():
    """List all jobs."""
    # Get pagination parameters
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    
    # Calculate offset
    offset = (page - 1) * per_page
    
    # Get jobs
    jobs = db.get_all_jobs(limit=per_page, offset=offset)
    
    return render_template(
        'jobs.html',
        jobs=jobs,
        page=page,
        per_page=per_page,
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )

@app.route('/api/prompts')
def api_list_prompts():
    """API endpoint to get available prompts."""
    prompts = list_available_prompts()
    return jsonify(prompts)

@app.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'version': '1.0',
        'time': datetime.now().isoformat()
    })

@app.errorhandler(404)
def page_not_found(e):
    """Handle 404 errors."""
    return render_template('error.html', error="Page not found"), 404

@app.errorhandler(500)
def server_error(e):
    """Handle 500 errors."""
    logger.error(f"Server error: {str(e)}")
    return render_template('error.html', error="Server error"), 500


# Create required templates directory
os.makedirs(os.path.join(os.path.dirname(__file__), 'templates'), exist_ok=True)
os.makedirs(os.path.join(os.path.dirname(__file__), 'static'), exist_ok=True)

def is_valid_url(url):
    """
    Check if a string is a valid URL.
    
    Args:
        url: String to check
        
    Returns:
        True if valid URL, False otherwise
    """
    import re
    if not url or not isinstance(url, str):
        return False
        
    pattern = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)?$', re.IGNORECASE)  # path
    
    return bool(re.match(pattern, url))

def get_status_description(status):
    """Get a human-readable description for a job status."""
    descriptions = {
        'pending': 'Job is waiting to be processed.',
        'running': 'Job is currently being processed.',
        'completed': 'Job completed successfully.',
        'completed_with_errors': 'Job completed but encountered some errors.',
        'failed': 'Job failed to complete.',
        'cancelled': 'Job was cancelled.'
    }
    return descriptions.get(status, f"Status: {status}")


if __name__ == '__main__':
    # Run the Flask app
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") == "development"
    
    logger.info(f"Starting Flask app on port {port} (debug={debug})")
    app.run(host='0.0.0.0', port=port, debug=debug)
