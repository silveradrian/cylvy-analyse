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
from analyzer import ContentAnalyzer, URLProcessor
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

def process_urls_in_background(job_id: str, urls: List[str], prompt_names: List[str], 
                            company_info: Optional[Dict[str, Any]] = None):
    """
    Process a list of URLs in the background.
    """
    try:
        logger.info(f"Starting background processing for job {job_id} with {len(urls)} URLs")
        
        # Fix 1: Use get_running_loop or create a new one properly
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Fix 2: Initialize error_count at the start
        error_count = 0
        processed_count = 0
        
        # Update job status to running
        db.update_job_status(
            job_id=job_id,
            status="running",
            total_urls=len(urls),
            processed_urls=0,
            error_count=0
        )
        
        # Process each URL
        for url in urls:
            try:
                # Check if the job has been cancelled
                job = db.get_job(job_id)
                if job and job.get('status') == 'cancelled':
                    logger.info(f"Job {job_id} was cancelled - stopping processing")
                    break
                
                # Process the URL - handle URLs as strings or dicts
                url_string = url if isinstance(url, str) else url.get('url')
                
                # Process the URL
                result = analyzer.process_url(url_string, prompt_names, company_info)
                
                # Fix 3: Ensure result has job_id for database storage
                result['job_id'] = job_id
                
                # Save the result
                db.save_result(result)  # Updated to match your current db_manager API
                
                # Update counters
                processed_count += 1
                if result.get('status') in ['error', 'scrape_error', 'analysis_error']:
                    error_count += 1
                
                # Update job status
                db.update_job_status(
                    job_id=job_id,
                    status="running",
                    processed_urls=processed_count,
                    error_count=error_count
                )
                
                logger.info(f"Processed URL {processed_count}/{len(urls)} for job {job_id}")
                
            except Exception as e:
                logger.error(f"Error processing URL {url} for job {job_id}: {str(e)}")
                error_count += 1
                
                # Update job status
                db.update_job_status(
                    job_id=job_id,
                    status="running",
                    processed_urls=processed_count,
                    error_count=error_count
                )
        
        # Mark the job as completed
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
            completed_at=datetime.now().isoformat()
        )
        
        logger.info(f"Completed background processing for job {job_id} - status: {final_status}")
        return True
        
    except Exception as e:
        logger.error(f"Error in background processing for job {job_id}: {str(e)}")
        
        # Mark the job as failed - fix: use local error_count or default to 1
        db.update_job_status(
            job_id=job_id,
            status="failed",
            error_count=error_count if 'error_count' in locals() else 1
        )
        return False
    finally:
        # Remove the thread from active threads
        if threading.current_thread().ident in active_threads:
            active_threads.remove(threading.current_thread().ident)


def process_urls_in_parallel(job_id: str, urls, prompt_names: List[str],
                           force_browser: bool = False, premium_proxy: bool = False):
    """Process URLs in parallel with direct ContentAnalyzer usage."""
    try:
        logger.info(f"Starting parallel processing for job {job_id} with {len(urls)} URLs")
        
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Initialize counters
        total_urls = len(urls)
        processed_count = error_count = 0
        
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
        max_concurrency = int(os.environ.get("MAX_CONCURRENCY", "3"))
        
        async def process_urls():
            semaphore = asyncio.Semaphore(max_concurrency)
            nonlocal processed_count, error_count
            
            async def process_with_limit(url_data):
                async with semaphore:
                    # Extract URL data
                    url = url_data.get('url') if isinstance(url_data, dict) else url_data
                    company_info = url_data.get('company_info') if isinstance(url_data, dict) else None
                    content_type = url_data.get('content_type', 'html') if isinstance(url_data, dict) else 'html'
                    url_force_browser = url_data.get('force_browser', force_browser) if isinstance(url_data, dict) else force_browser
                    
                    try:
                        # Process URL
                        result = await analyzer_instance.process_url_async(
                            url=url,
                            prompt_names=prompt_names,
                            company_info=company_info,
                            content_type=content_type,
                            force_browser=url_force_browser
                        )
                        result['job_id'] = job_id
                        return result
                    except Exception as e:
                        logger.error(f"Error processing URL {url}: {str(e)}")
                        return {
                            "url": url,
                            "job_id": job_id,
                            "status": "error",
                            "error": str(e),
                            "processed_at": time.time()
                        }
            
            # Create tasks for all URLs
            tasks = []
            for url_item in urls:
                if isinstance(url_item, str):
                    tasks.append(process_with_limit({'url': url_item}))
                else:
                    tasks.append(process_with_limit(url_item))
            
            # Process URLs in parallel with controlled concurrency
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Exception in URL processing: {str(result)}")
                    error_count += 1
                    continue
                
                try:
                    # Save to database
                    db.save_result(result)
                    processed_count += 1
                    
                    # Update error count if needed
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
            
            # Final job status
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
        
        # Run the async function
        processed, errors = loop.run_until_complete(process_urls())
        
        logger.info(f"Completed processing for job {job_id}: {processed}/{total_urls} URLs processed, {errors} errors")
        return True
    
    except Exception as e:
        logger.error(f"Error in parallel processing for job {job_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        try:
            db.update_job_status(
                job_id=job_id,
                status="failed",
                error_count=1
            )
        except Exception as db_error:
            logger.error(f"Failed to update job status: {str(db_error)}")
            
        return False
    
    finally:
        # Clean up
        if 'loop' in locals() and loop is not None:
            loop.close()
        
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

@app.route('/api/debug/job/<job_id>/data')
def debug_job_data(job_id):
    """Debug endpoint to examine the data structure for a job."""
    try:
        results = db.get_results_for_job(job_id)
        if not results:
            return jsonify({"error": "No results found"})
            
        # Get the first result
        result = results[0]
        
        # Basic info
        response = {
            "job_id": job_id,
            "result_keys": list(result.keys()),
            "result_values": {}
        }
        
        # Add basic values
        for key in ['url', 'status', 'title', 'word_count', 'content_type']:
            if key in result:
                response["result_values"][key] = result[key]
        
        # Check data field
        if 'data' in result:
            data_value = result['data']
            response["data_type"] = str(type(data_value))
            response["data_sample"] = str(data_value)[:500] if isinstance(data_value, str) else "Not a string"
            
            # Try to parse if it's a string
            if isinstance(data_value, str):
                try:
                    parsed = json.loads(data_value)
                    response["parsed_data_keys"] = list(parsed.keys()) if isinstance(parsed, dict) else "Not a dict"
                    
                    # Check for structured data
                    if isinstance(parsed, dict) and 'structured_data' in parsed:
                        struct_data = parsed['structured_data']
                        response["structured_data_keys"] = list(struct_data.keys()) if isinstance(struct_data, dict) else "Not a dict"
                        
                        if isinstance(struct_data, dict):
                            for prompt_name, fields in struct_data.items():
                                response[f"prompt_{prompt_name}_field_count"] = len(fields) if isinstance(fields, dict) else "Not a dict"
                                response[f"prompt_{prompt_name}_fields"] = list(fields.keys())[:10] if isinstance(fields, dict) else "Not a dict"
                except Exception as e:
                    response["json_parse_error"] = str(e)
        
        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)})


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




@app.route('/analyze', methods=['POST'])
def analyze():
    """
    Handle form submission for URL analysis with parallel processing.
    """
    try:
        # Get form data
        urls_input = request.form.get('urls', '').strip()
        prompt_names = request.form.getlist('prompts')
        force_browser = 'force_browser' in request.form
        premium_proxy = 'premium_proxy' in request.form
        
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
                url_data_list = [{'url': url} for url in urls]
        else:
            # Parse URLs from text input
            urls = [line.strip() for line in urls_input.split('\n') if line.strip()]
            url_data_list = [{'url': url} for url in urls]
        
        # Filter out invalid URLs
        valid_url_data_list = []
        for data in url_data_list:
            if is_valid_url(data.get('url', '')):
                valid_url_data_list.append(data)
            else:
                logger.warning(f"Invalid URL skipped: {data.get('url', '(empty)')}")
        
        if not valid_url_data_list:
            flash('No valid URLs provided.', 'danger')
            return redirect(url_for('index'))
        
        if not prompt_names:
            flash('Please select at least one prompt configuration.', 'warning')
            return redirect(url_for('index'))
        
        # Create a new job - only use parameters accepted by create_job()
        job_id = db.create_job(
            urls=[data.get('url') for data in valid_url_data_list],
            prompts=prompt_names
        )
        
        # We don't need to store these in the database separately since
        # the URL data objects already contain this information
        logger.info(f"Job {job_id} created with force_browser={force_browser}, premium_proxy={premium_proxy}")
        
        # Start processing in a background thread WITH PARALLEL PROCESSING
        thread = threading.Thread(
            target=process_urls_in_parallel,
            args=(job_id, valid_url_data_list, prompt_names, force_browser, premium_proxy)
        )
        thread.daemon = True
        thread.start()
        
        # Add thread ID to active threads
        active_threads.add(thread.ident)
        
        # Redirect to job status page
        return redirect(url_for('job_status', job_id=job_id))
            
    except Exception as e:
        logger.error(f"Error starting analysis: {str(e)}")
        flash(f"Error: {str(e)}", 'danger')
        return redirect(url_for('index'))


def parse_csv_file(file):
    """Parse a CSV file for URLs with company context."""
    import csv
    from io import StringIO
    import re
    import logging
    
    logger = logging.getLogger("app")
    
    url_data_list = []
    content = file.read().decode('utf-8-sig')  # Use utf-8-sig to handle BOM character
    
    # Get reader
    reader = csv.DictReader(StringIO(content))
    
    # Find the URL column - handle BOM character
    url_column = None
    for header in reader.fieldnames:
        if header.strip('\ufeff') == 'url':  # Strip BOM character
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
                    if header.strip('\ufeff') == csv_field:
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
                if header.strip('\ufeff') == 'content_type':
                    content_type_col = header
                    break
                    
            if content_type_col and row[content_type_col]:
                url_data['content_type'] = row[content_type_col]
            
            # Add browser flag if available
            force_browser_col = None
            for header in row.keys():
                if header.strip('\ufeff') == 'force_browser':
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
    try:
        # Get job details
        job = db.get_job(job_id)
        
        if not job:
            flash("Job not found", "error")
            return redirect(url_for('index'))
        
        # Get export format
        export_format = request.args.get('format', 'csv')
        
        if export_format == 'csv':
            # Export to CSV
            csv_path = db.export_results_to_csv(job_id)
            
            if not csv_path:
                flash("Error generating CSV export", "danger")
                return redirect(url_for('job_status', job_id=job_id))
            
            logger.info(f"Sending CSV file from {csv_path}")
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
                flash("Error generating Excel export", "danger")
                return redirect(url_for('job_status', job_id=job_id))
            
            logger.info(f"Sending Excel file from {excel_path}")
            return send_file(
                excel_path,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f"results_{job_id}.xlsx"
            )
        else:
            flash("Invalid export format", "warning")
            return redirect(url_for('job_status', job_id=job_id))
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        flash(f"Export failed: {str(e)}", "danger")
        return redirect(url_for('job_status', job_id=job_id))
    
    
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
    """Check if a string is a valid URL."""
    import re
    import logging
    
    logger = logging.getLogger("app")
    
    if not url or not isinstance(url, str):
        return False
    
    # Remove any whitespace
    url = url.strip()
    
    # Add http:// prefix if missing
    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"
        
    # Common URL validation pattern
    pattern = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)?$', re.IGNORECASE)  # optional path
    
    is_valid = bool(re.match(pattern, url))
    
    if not is_valid:
        logger.info(f"URL validation failed: {url}")
    
    return is_valid

@app.route('/debug-csv')
def debug_csv_page():
    return render_template('debug.html')

@app.route('/api/debug/parse-csv', methods=['POST'])
def debug_parse_csv():
    """Debug endpoint to test CSV parsing."""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
            
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400
            
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'File must be a CSV'}), 400
        
        # Parse CSV using the same function as the analyze endpoint
        url_data_list = parse_csv_file(file)
        
        # Check for valid URLs
        valid_urls = []
        invalid_urls = []
        
        for data in url_data_list:
            url = data.get('url', '')
            if is_valid_url(url):
                valid_urls.append(data)
            else:
                invalid_urls.append(url)
        
        return jsonify({
            'total_rows': len(url_data_list),
            'valid_urls': len(valid_urls),
            'invalid_urls': len(invalid_urls),
            'invalid_url_examples': invalid_urls[:5],
            'first_valid_data': valid_urls[0] if valid_urls else None
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug/csv-upload', methods=['POST'])
def debug_csv_upload():
    """Debug endpoint for CSV upload issues."""
    try:
        import logging
        logger = logging.getLogger("app")
        
        if 'file' not in request.files:
            return jsonify({"error": "No file part"}), 400
        
        file = request.files['file']
        
        # Read and parse CSV
        content = file.read().decode('utf-8')
        file.seek(0)  # Rewind file
        
        # Get CSV headers
        import csv
        from io import StringIO
        reader = csv.DictReader(StringIO(content))
        headers = reader.fieldnames
        
        # Parse again to get URLs
        file.seek(0)
        url_data_list = parse_csv_file(file)
        
        # Validate each URL and collect results
        validation_results = []
        for item in url_data_list:
            url = item.get('url', '')
            is_valid = is_valid_url(url)
            validation_results.append({
                'url': url, 
                'valid': is_valid
            })
        
        # Return debug info
        return jsonify({
            "success": True,
            "csv_headers": headers,
            "csv_preview": content[:500],
            "parsed_urls": len(url_data_list),
            "validation_results": validation_results,
            "first_url_data": url_data_list[0] if url_data_list else None
        })
    except Exception as e:
        logger.exception("Error in debug endpoint")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == '__main__':
    # Run the Flask app
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") == "development"
    
    logger.info(f"Starting Flask app on port {port} (debug={debug})")
    app.run(host='0.0.0.0', port=port, debug=debug)

