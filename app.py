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
from flask import Flask, request, jsonify, render_template, redirect, url_for, send_file, Response

# Import our custom modules
from db_manager import db
from analyzer import ContentAnalyzer
from prompt_loader import list_available_prompts, get_prompt_by_name

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
        
        # Ensure we have an event loop
       
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Update job status to running
        db.update_job_status(
            job_id=job_id,
            status="running",
            total_urls=len(urls),
            processed_urls=0,
            error_count=0
        )
        
        processed_count = 0
        error_count = 0
        
        # Process each URL
        for url in urls:
            try:
                # Check if the job has been cancelled
                job = db.get_job(job_id)
                if job and job.get('status') == 'cancelled':
                    logger.info(f"Job {job_id} was cancelled - stopping processing")
                    break
                
                # Process the URL
                result = analyzer.process_url(url, prompt_names, company_info)
                
                # Save the result
                db.save_result(job_id, result)
                
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
            error_count=error_count
        )
        
        logger.info(f"Completed background processing for job {job_id} - status: {final_status}")
        
    except Exception as e:
        logger.error(f"Error in background processing for job {job_id}: {str(e)}")
        
        # Mark the job as failed
        db.update_job_status(
            job_id=job_id,
            status="failed",
            error_count=error_count + 1
        )
    finally:
        # Remove the thread from active threads
        if threading.current_thread().ident in active_threads:
            active_threads.remove(threading.current_thread().ident)

@app.template_filter('format_date')
def format_date(value, format='%Y-%m-%d %H:%M:%S'):
    """Format a date time to a readable format."""
    if value is None:
        return ""
    
    if isinstance(value, str):
        try:
            # Try to parse ISO format
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            # If parse fails, return as is
            return value
    else:
        dt = value
    
    # You can adjust timezone if needed
    # dt = dt.astimezone(pytz.timezone('UTC'))
    return dt.strftime(format)



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

@app.route('/analyze', methods=['POST'])
def analyze():
    """
    Handle form submission for URL analysis.
    """
    try:
        # Get form data
        urls_input = request.form.get('urls', '').strip()
        prompt_names = request.form.getlist('prompts')
        
        # Check input file first (prioritize file over text input)
        if 'file' in request.files and request.files['file'].filename:
            file = request.files['file']
            
            # Check file extension
            if file.filename.endswith('.csv'):
                # Parse CSV file for URLs with company context
                url_data_list = parse_csv_file(file)
            elif file.filename.endswith(('.xlsx', '.xls')):
                # Parse Excel file for URLs with company context
                url_data_list = parse_excel_file(file)
            elif file.filename.endswith('.json'):
                # Parse JSON file for URLs with company context
                url_data_list = parse_json_file(file)
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
        url_data_list = [data for data in url_data_list if is_valid_url(data.get('url', ''))]
        
        if not url_data_list:
            flash('No valid URLs provided.')
            return redirect(url_for('index'))
        
        if not prompt_names:
            flash('Please select at least one prompt configuration.')
            return redirect(url_for('index'))
        
        # Create a new job
        job_id = db.create_job(
            urls=[data.get('url') for data in url_data_list],
            prompts=prompt_names
        )
        
        # Start processing in the background
        processing_started = process_urls_in_background(job_id, url_data_list, prompt_names)
        
        if processing_started:
            return redirect(url_for('job_status', job_id=job_id))
        else:
            flash('Failed to start analysis job.')
            return redirect(url_for('index'))
            
    except Exception as e:
        logger.error(f"Error starting analysis: {str(e)}")
        flash(f"Error: {str(e)}")
        return redirect(url_for('index'))


def parse_csv_file(file):
    """Parse a CSV file for URLs with company context."""
    import csv
    from io import StringIO
    
    url_data_list = []
    content = file.read().decode('utf-8')
    reader = csv.DictReader(StringIO(content))
    
    for row in reader:
        if 'url' in row:
            url_data = {'url': row['url']}
            
            # Extract company info if available
            company_info = {}
            for key in ['company_name', 'company_description', 'industry', 'revenue']:
                if key in row and row[key]:
                    # Convert keys to match expected format
                    company_key = key.replace('company_', '')
                    company_info[company_key] = row[key]
            
            # Add company info if any fields were found
            if company_info:
                url_data['company_info'] = company_info
            
            # Add content type and rendering options if available
            if 'content_type' in row:
                url_data['content_type'] = row['content_type']
            if 'force_browser' in row:
                url_data['force_browser'] = row['force_browser'].lower() in ('true', 'yes', '1')
            
            url_data_list.append(url_data)
    
    return url_data_list

@app.route('/job/<string:job_id>')
def job_status(job_id):
    """
    Display job status page.
    """
    job = db.get_job(job_id)
    
    if not job:
        return render_template('error.html', message=f"Job {job_id} not found"), 404
    
    results = db.get_results_for_job(job_id)
    
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

if __name__ == '__main__':
    # Run the Flask app
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") == "development"
    
    logger.info(f"Starting Flask app on port {port} (debug={debug})")
    app.run(host='0.0.0.0', port=port, debug=debug)
