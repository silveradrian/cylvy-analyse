"""
Module for handling local files in the Cylvy analysis system
"""
import os
import time
import threading
import logging
import asyncio
from typing import Dict, List, Any
from db_manager import db

# Configure logging
logger = logging.getLogger(__name__)

def read_file_content(file_path):
    """Read the content of a file based on its extension"""
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    
    # Binary files need to be read in 'rb' mode
    if ext in ['.pdf', '.docx', '.pptx']:
        with open(file_path, 'rb') as file:
            return file.read()
    else:
        # Text files can be read in 'r' mode
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except UnicodeDecodeError:
            # Fallback to binary if text read fails
            with open(file_path, 'rb') as file:
                return file.read()

def call_async_function(async_func, *args, **kwargs):
    """Run an async function from sync code using a new event loop"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(async_func(*args, **kwargs))
    finally:
        loop.close()

def process_local_files(job_id, file_paths, prompt_names, active_threads=None):
    """
    Process local files directly using the ContentAnalyzer
    but with file reading instead of HTTP requests
    """
    from analyzer import ContentAnalyzer  # Import here to avoid circular imports
    import json
    
    # Create analyzer instance
    analyzer = ContentAnalyzer()
    
    # Track progress
    processed_count = 0
    error_count = 0
    total_files = len(file_paths)
    
    # Update initial job status
    db.update_job_status(
        job_id=job_id,
        status="running",
        total_urls=total_files,
        processed_urls=0,
        error_count=0
    )
    
    # Process each file
    for idx, file_info in enumerate(file_paths):
        try:
            file_path = file_info['path']
            content_type = file_info.get('content_type', 'html')
            file_name = os.path.basename(file_path)
            
            logger.info(f"Processing local file: {file_name} ({content_type})")
            
            # Extract text using the appropriate method based on file type
            result = None
            
            if content_type == 'pdf':
                result = analyzer.extract_from_pdf(file_path)
            elif content_type == 'docx':
                result = analyzer.extract_from_docx(file_path)
            elif content_type == 'pptx':
                result = analyzer.extract_from_pptx(file_path)
            else:
                # For HTML/text files, read content and treat as HTML
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    
                # Create a basic result with the text content
                result = {
                    "text": content,
                    "title": file_name,
                    "status": "success",
                    "content_type": "html",
                    "word_count": len(content.split()),
                    "method_used": "direct_file_read"
                }
            
            # Use the analyzer to process the content with the prompt
            if result and result.get("status") == "success":
                # Create URL identifier for database (keep file:// format for consistency)
                url_id = f"file://{file_path}"
                
                # Process the extracted content with the prompt
                analysis_results = {}
                api_tokens = 0
                
                # Process with each requested prompt
                for prompt_name in prompt_names:
                    # Get prompt config from prompt_loader
                    from prompt_loader import PromptLoader
                    prompt_loader = PromptLoader()
                    prompt_configs = prompt_loader.load_prompts()
                    
                    if prompt_name in prompt_configs:
                        prompt_config = prompt_configs[prompt_name]
                        
                        # Call the async function properly using our helper function
                        analysis = call_async_function(
                            analyzer.analyze_with_prompt_async,
                            result.get("text", ""),
                            prompt_config,
                            {"filename": file_name}
                        )
                        
                        # Store analysis
                        if analysis and not analysis.get("error"):
                            analysis_results[prompt_name] = analysis
                            api_tokens += analysis.get("total_tokens", 0)
                    else:
                        logger.warning(f"Prompt config not found: {prompt_name}")
                
                # Create data object with analysis results
                data_obj = {
                    "analysis_results": analysis_results,
                    "structured_data": {}
                }
                
                # Create final result with all information
                final_result = {
                    "job_id": job_id,
                    "url": url_id,
                    "title": result.get("title", file_name),
                    "status": "success",
                    "content_type": content_type,
                    "word_count": result.get("word_count", 0),
                    "processed_at": time.time(),
                    "prompt_name": prompt_names[0] if prompt_names else "",
                    "api_tokens": api_tokens,
                    "error": "",
                    "data": json.dumps(data_obj)
                }
                
                # Save to database
                db.save_result(final_result)
                
                processed_count += 1
                logger.info(f"Successfully processed file {idx+1}/{total_files}: {file_name}")
                
            else:
                # Handle extraction errors
                error_msg = result.get("error", "Unknown error during file extraction")
                error_result = {
                    "job_id": job_id,
                    "url": f"file://{file_path}",
                    "title": file_name,
                    "status": "error",
                    "content_type": content_type,
                    "word_count": 0,
                    "processed_at": time.time(),
                    "prompt_name": prompt_names[0] if prompt_names else "",
                    "api_tokens": 0,
                    "error": error_msg,
                    "data": "{}"
                }
                
                db.save_result(error_result)
                error_count += 1
                processed_count += 1
                logger.error(f"Error processing file {file_name}: {error_msg}")
            
        except Exception as e:
            import traceback
            logger.error(f"Error processing file {file_path}: {str(e)}")
            logger.error(traceback.format_exc())
            
            error_count += 1
            processed_count += 1
            
            # Save error result
            error_result = {
                "job_id": job_id,
                "url": f"file://{file_path}",
                "title": os.path.basename(file_path),
                "status": "error",
                "content_type": file_info.get('content_type', 'unknown'),
                "word_count": 0,
                "processed_at": time.time(),
                "prompt_name": prompt_names[0] if prompt_names else "",
                "api_tokens": 0,
                "error": str(e),
                "data": "{}"
            }
            db.save_result(error_result)
        
        # Update job status
        db.update_job_status(
            job_id=job_id,
            status="running",
            processed_urls=processed_count,
            error_count=error_count
        )
    
    # Update final status
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
    
    # Remove thread from active threads
    if active_threads and threading.current_thread().ident in active_threads:
        active_threads.remove(threading.current_thread().ident)
        
    logger.info(f"Completed processing {total_files} local files. Success: {processed_count-error_count}, Errors: {error_count}")
