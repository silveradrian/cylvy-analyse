from __future__ import annotations  # This must be the first import!

import asyncio
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import threading
from bs4 import BeautifulSoup
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import aiohttp
import async_timeout

# Third-party imports
from openai import OpenAI, AsyncOpenAI
import tiktoken
from scrapingbee import ScrapingBeeClient
from limits import RateLimitItem, storage
from limits.strategies import FixedWindowRateLimiter
from limits.aio.strategies import FixedWindowRateLimiter as AsyncFixedWindowRateLimiter
from aiolimiter import AsyncLimiter

# Internal imports
from prompt_loader import PromptLoader
from db_manager import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize OpenAI clients
openai_api_key = os.environ.get("OPENAI_API_KEY", "")
client = OpenAI(api_key=openai_api_key)
async_client = AsyncOpenAI(api_key=openai_api_key)

def parse_structured_response(analysis_text, prompt_config):
    """
    Parse a structured response from OpenAI API based on the prompt configuration.
    
    Args:
        analysis_text: The raw text response from OpenAI
        prompt_config: The prompt configuration with output_fields and delimiter
        
    Returns:
        Dictionary with parsed field values
    """
    parsed_fields = {}
    delimiter = prompt_config.get('delimiter', '|||')
    output_fields = prompt_config.get('output_fields', [])
    
    # Log the first few lines of the response for debugging
    preview_lines = analysis_text.strip().split('\n')[:10]
    logger.warning(f"No fields parsed from response. First few lines:\n{preview_lines[0]}")
    
    # Create a mapping of field names to their types
    field_types = {field.get('name'): field.get('field_type', 'text') 
                  for field in output_fields if 'name' in field}
    
    # Split the response into lines
    lines = analysis_text.strip().split('\n')
    
    for line in lines:
        if delimiter in line:
            # Split at the delimiter
            parts = line.split(delimiter, 1)
            if len(parts) == 2:
                field_name = parts[0].strip()
                field_value = parts[1].strip()
                
                # Get field type from configuration
                field_type = field_types.get(field_name, 'text')
                
                # Convert value based on field type
                if field_type in ('int', 'integer'):
                    try:
                        # Handle values like [1-10 or 0] by extracting the first number
                        if '[' in field_value and ']' in field_value:
                            import re
                            numbers = re.findall(r'\d+', field_value)
                            if numbers:
                                parsed_fields[field_name] = int(numbers[0])
                            else:
                                parsed_fields[field_name] = 0
                        else:
                            parsed_fields[field_name] = int(field_value)
                    except (ValueError, TypeError):
                        parsed_fields[field_name] = 0
                        
                elif field_type in ('float', 'number'):
                    try:
                        parsed_fields[field_name] = float(field_value)
                    except (ValueError, TypeError):
                        parsed_fields[field_name] = 0.0
                        
                elif field_type == 'boolean':
                    parsed_fields[field_name] = field_value.lower() in ('true', 'yes', '1')
                    
                elif field_type == 'list':
                    if field_value == "None" or field_value == "[None]":
                        parsed_fields[field_name] = []
                    elif field_value.startswith('[') and field_value.endswith(']'):
                        # Try to parse as JSON list
                        try:
                            import json
                            parsed_fields[field_name] = json.loads(field_value.replace("'", "\""))
                        except json.JSONDecodeError:
                            # Fall back to simple string splitting
                            items = field_value[1:-1].split(',')
                            parsed_fields[field_name] = [item.strip().strip("'\"") for item in items]
                    else:
                        # Just store as a single-item list
                        parsed_fields[field_name] = [field_value]
                else:
                    # Default to text
                    parsed_fields[field_name] = field_value
    
    logger.info(f"Parsed {len(parsed_fields)} fields from response")
    return parsed_fields

class AdvancedRateLimiter:
    """Advanced rate limiter using simple time-based throttling"""
    
    def __init__(self, rate_limit_per_minute: int = 60):
        self.rate_limit_per_minute = rate_limit_per_minute
        self.request_timestamps = []
        self.lock = threading.Lock()  # Add thread safety
        logger.info(f"Advanced rate limiter initialized: {rate_limit_per_minute} requests/minute")
    
    def __enter__(self):
        """Wait until a request is allowed based on the rate limit"""
        with self.lock:
            # Clean up old timestamps (older than 60 seconds)
            current_time = time.time()
            self.request_timestamps = [ts for ts in self.request_timestamps 
                                     if current_time - ts < 60]
            
            # Wait if we've hit the limit
            if len(self.request_timestamps) >= self.rate_limit_per_minute:
                # Calculate time to wait - the oldest timestamp + 60s should expire
                oldest = min(self.request_timestamps)
                wait_time = (oldest + 60) - current_time
                if wait_time > 0:
                    logger.info(f"Rate limit hit. Waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                
                # Clean up again after waiting
                current_time = time.time()
                self.request_timestamps = [ts for ts in self.request_timestamps 
                                         if current_time - ts < 60]
            
            # Record this request
            self.request_timestamps.append(current_time)
            return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class AsyncAdvancedRateLimiter:
    """Async advanced rate limiter for high throughput operations"""
    
    def __init__(self, rate_limit_per_minute: int = 60):
        """Initialize with requests per minute limit"""
        self.rate_limit_per_minute = rate_limit_per_minute
        logger.info(f"Async rate limiter initialized: {rate_limit_per_minute} requests/minute")
    
    async def __aenter__(self):
        """Always allow operations to proceed without blocking"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager"""
        pass
    
class ContentAnalyzer:
    """
    Analyzes content from URLs using OpenAI's API.
    """
    
    def __init__(self):
        """Initialize the content analyzer."""
        try:
            # Try to load environment variables from .env file
            try:
                from dotenv import load_dotenv
                load_dotenv(override=True)
                logger.info("Loaded environment from .env file")
            except ImportError:
                logger.warning("python-dotenv not installed, skipping .env loading")
            
            # Get API keys
            self.openai_api_key = os.environ.get("OPENAI_API_KEY", "")
            self.scrapingbee_api_key = os.environ.get("SCRAPINGBEE_API_KEY", "")
            
            # Log API key status
            if not self.openai_api_key:
                logger.error("OPENAI_API_KEY not found in environment variables!")
                # List environment vars for debugging (omitting actual values)
                env_vars = [k for k in os.environ.keys() if 'API' in k or 'KEY' in k]
                logger.info(f"Available environment variables: {env_vars}")
            else:
                logger.info(f"OpenAI API key found (length: {len(self.openai_api_key)})")
            
            if not self.scrapingbee_api_key:
                logger.warning("SCRAPINGBEE_API_KEY not found in environment variables!")
            else:
                logger.info(f"ScrapingBee API key found (length: {len(self.scrapingbee_api_key)})")
            
            # Initialize OpenAI clients
            self.openai_client = OpenAI(api_key=self.openai_api_key)
            self.async_openai_client = AsyncOpenAI(api_key=self.openai_api_key)
            
            # Initialize ScrapingBee client
            self.scrapingbee_client = ScrapingBeeClient(api_key=self.scrapingbee_api_key)
            
            # Set up rate limiters with simplified implementations
            openai_rate_limit = int(os.environ.get("OPENAI_RATE_LIMIT", "60"))
            scrapingbee_rate_limit = int(os.environ.get("SCRAPINGBEE_RATE_LIMIT", "50"))
            
            self.openai_rate_limiter = AdvancedRateLimiter(openai_rate_limit)
            self.async_openai_rate_limiter = AsyncAdvancedRateLimiter(openai_rate_limit)
            self.scrapingbee_rate_limiter = AdvancedRateLimiter(scrapingbee_rate_limit)
            self.async_scrapingbee_rate_limiter = AsyncAdvancedRateLimiter(scrapingbee_rate_limit)
            
            logger.info(f"Content analyzer initialized with advanced rate limiters")
            logger.info(f"OpenAI API key present: {bool(self.openai_api_key)}")
            logger.info(f"OpenAI rate limit: {openai_rate_limit} requests/minute")
            logger.info(f"ScrapingBee rate limit: {scrapingbee_rate_limit} requests/minute")
            
        except Exception as e:
            logger.error(f"Error initializing ContentAnalyzer: {str(e)}")
            # Still create default rate limiters even if initialization fails
            self.openai_rate_limiter = AdvancedRateLimiter(60)
            self.async_openai_rate_limiter = AsyncAdvancedRateLimiter(60)
            self.scrapingbee_rate_limiter = AdvancedRateLimiter(50)
            self.async_scrapingbee_rate_limiter = AsyncAdvancedRateLimiter(50)
    
    async def scrape_url_async(self, url: str, content_type: str = "html", 
                    force_browser: bool = False) -> Dict[str, Any]:
        """
        Asynchronously scrape content from a URL using ScrapingBee's API.
        """
        try:
            logger.info(f"Async scraping URL with ScrapingBee: {url}")
            
            # Only handle HTML for now
            if content_type.lower() != 'html':
                logger.warning(f"Skipping non-HTML content type: {content_type}")
                return {
                    "error": f"Only HTML content is currently supported",
                    "url": url,
                    "status": "error"
                }
            
            # Reload the API key each time
            scrapingbee_api_key = os.environ.get("SCRAPINGBEE_API_KEY", "")
            
            # Create a fresh client instance for this request
            scrapingbee_client = ScrapingBeeClient(api_key=scrapingbee_api_key)
            
            if not scrapingbee_api_key:
                logger.error(f"Missing ScrapingBee API key for {url}")
                return {
                    "error": "ScrapingBee API key not configured",
                    "url": url,
                    "status": "error"
                }
            
            start_time = time.time()
            
            # Skip the rate limiter and execute directly
            # Use ThreadPoolExecutor since ScrapingBee client is not async
            with ThreadPoolExecutor() as executor:
                loop = asyncio.get_event_loop()
                
                params = {
                    "render_js": "true" if force_browser else "false",
                    "premium_proxy": "true" if force_browser else "false"
                }
                
                response = await loop.run_in_executor(
                    executor,
                    lambda: scrapingbee_client.get(url, params=params)
                )
            
            duration = time.time() - start_time
            
            # Check response status
            if response.status_code != 200:
                logger.error(f"ScrapingBee error: {response.status_code} - {response.text}")
                return {
                    "error": f"ScrapingBee returned status code {response.status_code}: {response.text}",
                    "url": url,
                    "status": "error"
                }
            
            # Process HTML content
            html_content = response.content.decode('utf-8', errors='ignore')
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract title
            title = ""
            if soup.title:
                title = soup.title.text.strip()
            
            # Clean up the HTML
            for element in soup(["script", "style", "noscript", "iframe", "head"]):
                element.extract()
            
            # Get the text content
            text = soup.get_text(separator="\n", strip=True)
            
            # Count words
            word_count = len(text.split())
            
            result = {
                "url": url,
                "title": title,
                "text": text,
                "word_count": word_count,
                "status": "success",
                "content_type": "html",
                "scrape_time": duration
            }
            
            logger.info(f"Successfully scraped {url} - {result.get('word_count', 0)} words in {duration:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            return {
                "error": str(e),
                "url": url, 
                "status": "error"
            }

    async def analyze_batch_with_prompt(self, contents: List[str], prompt_config: Dict[str, Any],
                                      company_infos: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Analyze multiple contents with controlled concurrency.
        
        Args:
            contents: List of content texts to analyze
            prompt_config: Prompt configuration
            company_infos: Optional list of company context info (one per content)
            
        Returns:
            List of dictionaries containing analysis results
        """
        # Determine appropriate concurrency for API calls
        max_concurrency = int(os.environ.get("OPENAI_CONCURRENCY", "10"))
        
        # Ensure company_infos is the right length if provided
        if company_infos is None:
            company_infos = [None] * len(contents)
        elif len(company_infos) != len(contents):
            company_infos = company_infos + [None] * (len(contents) - len(company_infos))
        
        logger.info(f"Batch analyzing {len(contents)} contents with concurrency {max_concurrency}")
        
        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(max_concurrency)
        
        async def analyze_with_semaphore(content, company_info):
            async with semaphore:
                return await self.analyze_with_prompt_async(content, prompt_config, company_info)
        
        # Create tasks for all contents
        tasks = [analyze_with_semaphore(content, company_info) 
                for content, company_info in zip(contents, company_infos)]
        
        # Execute all tasks and gather results
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results, handling any exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error analyzing content #{i}: {str(result)}")
                processed_results.append({
                    "error": str(result),
                    "analysis": "Error: Could not complete analysis."
                })
            else:
                processed_results.append(result)
        
        return processed_results
    

    async def analyze_with_prompt_async(self, content: str, prompt_config: Dict[str, Any], 
                          company_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Asynchronously analyze content using OpenAI model with a specific prompt configuration.
        """
        try:
            start_time = time.time()
            
            # Force reload API key from environment before each API call
            api_key = os.environ.get("OPENAI_API_KEY", self.openai_api_key)
            
            # Make a direct client instance for this specific call
            async_client = AsyncOpenAI(api_key=api_key)
            
            if not api_key:
                logger.error("OpenAI API key missing for API call")
                return {
                    "error": "OpenAI API key missing",
                    "analysis": "Error: Could not complete analysis due to missing API key."
                }
                
            # Extract prompt configuration
            model = prompt_config.get('model', 'gpt-4')
            system_message = prompt_config.get('system_message', '')
            user_message_template = prompt_config.get('user_message', '')
            temperature = prompt_config.get('temperature', 0.3)
            max_tokens = prompt_config.get('max_tokens', 1500)
            
            # Prepare the company context
            company_context = ""
            if company_info:
                company_context = "Company Information:\n"
                for key, value in company_info.items():
                    company_context += f"- {key.capitalize()}: {value}\n"
            
            # Format user message with the content and company context
            # Fix: Use explicit named parameters and ensure content is properly included
            try:
                if "{content}" in user_message_template:
                    user_message = user_message_template.format(
                        content=content[:50000],  # Limit content length
                        company_context=company_context
                    )
                else:
                    # If there's no {content} placeholder, append content
                    user_message = user_message_template + "\n\nContent to analyze:\n" + content[:50000]
            except Exception as format_error:
                logger.error(f"Error formatting user message: {str(format_error)}")
                # Fallback to simple concatenation
                user_message = f"{user_message_template}\n\nContent to analyze:\n{content[:50000]}"
            
            # Log the request details
            logger.info(f"Calling OpenAI API with model {model} - content length: {len(content)} chars")
            
            # Skip rate limiter and make direct API call
            try:
                # Call the OpenAI API asynchronously
                response = await async_client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_message},
                        {"role": "user", "content": user_message}
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                
                # Calculate token usage
                prompt_tokens = response.usage.prompt_tokens
                completion_tokens = response.usage.completion_tokens
                total_tokens = response.usage.total_tokens
                
                # Extract the response
                analysis_text = response.choices[0].message.content
                
                # Parse the structured fields from the analysis text using our dedicated function
                parsed_fields = parse_structured_response(analysis_text, prompt_config)
                logger.info(f"Parsed {len(parsed_fields)} structured fields from the response")
                
                # Calculate processing time
                processing_time = time.time() - start_time
                
                logger.info(f"OpenAI API call completed in {processing_time:.2f}s")
                logger.info(f"Token usage: {total_tokens} tokens")
                
                # Return both the raw analysis and the parsed fields
                return {
                    "analysis": analysis_text,
                    "parsed_fields": parsed_fields,  # Add the structured fields
                    "model": model,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                    "processing_time": processing_time
                }
                
            except Exception as api_error:
                logger.error(f"Error calling OpenAI API: {str(api_error)}")
                return {
                    "error": f"API error: {str(api_error)}",
                    "analysis": "Error: Could not complete analysis due to API error."
                }
            
        except Exception as e:
            logger.error(f"Error in async OpenAI API call: {str(e)}")
            return {
                "error": str(e),
                "analysis": "Error: Could not complete analysis."
            }
    
    async def process_url_async(self, url: str, prompt_names: List[str], 
                             company_info: Optional[Dict[str, Any]] = None,
                             content_type: str = "html", 
                             force_browser: bool = False) -> Dict[str, Any]:
        """
        Asynchronously process a URL by scraping content and analyzing it.
        """
        logger.info(f"Async processing URL: {url} with {len(prompt_names)} prompts")
        
        # Load prompts
        prompt_loader = PromptLoader()
        prompt_configs = prompt_loader.load_prompts()
        
        result = {
            "url": url,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "scrape_time": 0,
            "analysis_time": 0,
            "total_time": 0,
            "analysis_results": {},
            "structured_data": {}  # Add a container for structured data
        }
        
        start_time = time.time()
        
        # Step 1: Scrape the URL
        try:
            content_result = await self.scrape_url_async(url, content_type, force_browser)
        except Exception as e:
            logger.error(f"Error in async scraping for {url}: {str(e)}")
            content_result = {
                "error": f"Error running scraping task: {str(e)}",
                "url": url,
                "status": "error"
            }
        
        # Check for scraping errors
        if content_result.get("status") != "success":
            result.update({
                "status": "scrape_error",
                "error": content_result.get("error", "Unknown scraping error"),
                "total_time": time.time() - start_time
            })
            return result
        
        # Update result with content info
        result.update({
            "title": content_result.get("title", ""),
            "content_type": content_result.get("content_type", "html"),
            "word_count": content_result.get("word_count", 0),
            "scrape_time": content_result.get("scrape_time", 0)
        })
        
        # Step 2: Analyze with each requested prompt
        content_text = content_result.get("text", "")
        api_tokens = 0
        
        for prompt_name in prompt_names:
            if prompt_name not in prompt_configs:
                logger.warning(f"Prompt configuration '{prompt_name}' not found")
                result["analysis_results"][prompt_name] = {
                    "error": f"Prompt configuration '{prompt_name}' not found"
                }
                continue
            
            try:
                # Perform analysis asynchronously
                prompt_config = prompt_configs[prompt_name]
                analysis_result = await self.analyze_with_prompt_async(
                    content_text, prompt_config, company_info
                )
                
                # Check for errors
                if "error" in analysis_result:
                    result["analysis_results"][prompt_name] = {
                        "error": analysis_result["error"]
                    }
                    continue
                
                # Add the analysis result
                result["analysis_results"][prompt_name] = {
                    "analysis": analysis_result.get("analysis", ""),
                    "model": analysis_result.get("model", ""),
                    "tokens": analysis_result.get("total_tokens", 0),
                    "processing_time": analysis_result.get("processing_time", 0),
                    "parsed_fields": analysis_result.get("parsed_fields", {})  # Include parsed fields here
                }
                
                # Add the parsed structured data to the result
                if "parsed_fields" in analysis_result:
                    result["structured_data"][prompt_name] = analysis_result["parsed_fields"]
                    logger.info(f"Added {len(analysis_result['parsed_fields'])} structured fields for prompt '{prompt_name}'")
                
                # Update token count
                api_tokens += analysis_result.get("total_tokens", 0)
                
                logger.info(f"Successfully analyzed content with prompt '{prompt_name}'")
                
            except Exception as e:
                logger.error(f"Error analyzing with prompt '{prompt_name}': {str(e)}")
                result["analysis_results"][prompt_name] = {
                    "error": str(e)
                }
        
        # Update final result
        total_time = time.time() - start_time
        result.update({
            "status": "success",
            "api_tokens": api_tokens,
            "analysis_time": total_time - result["scrape_time"],
            "total_time": total_time
        })
        
        logger.info(f"Completed processing {url} - status: {result['status']}")
        return result
    
    # Non-async versions for backward compatibility
    def scrape_url(self, url: str, content_type: str = "html", 
                force_browser: bool = False) -> Dict[str, Any]:
        """
        Synchronous wrapper for scrape_url_async.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(self.scrape_url_async(url, content_type, force_browser))
        finally:
            loop.close()
        return result
    
    def process_url(self, url: str, prompt_names: List[str], 
             company_info: Optional[Dict[str, Any]] = None,
             content_type: str = "html", 
             force_browser: bool = False) -> Dict[str, Any]:
        """
        Process a single URL by scraping content and analyzing it with selected prompts.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Get the raw result from async processing
            raw_result = loop.run_until_complete(
                self.process_url_async(url, prompt_names, company_info, content_type, force_browser)
            )
            
            # Current timestamp for processed_at
            current_time = time.time()
            
            # Extract the first prompt name (or default to empty string)
            prompt_name = prompt_names[0] if prompt_names else ""
            
            # Calculate API tokens used
            api_tokens = raw_result.get("api_tokens", 0)
            
            # Get the structured data from the analysis
            structured_data = raw_result.get("structured_data", {})
            
            # Combine analysis results and structured data
            combined_data = {
                "analysis_results": raw_result.get("analysis_results", {}),
                "structured_data": structured_data
            }
            
            # Format result to match database schema (results table)
            result = {
                'url': url,
                'status': raw_result.get('status', 'unknown'),
                'title': raw_result.get('title', ''),
                'content_type': raw_result.get('content_type', 'html'),
                'word_count': raw_result.get('word_count', 0),
                'processed_at': current_time,
                'prompt_name': prompt_name,
                'api_tokens': api_tokens,
                'error': raw_result.get('error', ''),
                'data': json.dumps(combined_data)  # Save combined data with structured fields
            }
            
            # Also add the structured fields directly to the result for easy access in exports
            if prompt_name in structured_data:
                logger.info(f"Adding {len(structured_data[prompt_name])} structured fields to result")
                for field_name, field_value in structured_data[prompt_name].items():
                    result[field_name] = field_value
                    logger.debug(f"Added field '{field_name}' with value: {field_value}")
            
            # Log successful processing
            logger.info(f"Processed URL {url} with {len(prompt_names)} prompts - Status: {result['status']}")
            
            return result
        
        except Exception as e:
            logger.error(f"Error in process_url for {url}: {str(e)}")
            # Return error result that matches database schema
            return {
                'url': url,
                'status': 'error',
                'title': '',
                'content_type': content_type,
                'word_count': 0,
                'processed_at': time.time(),
                'prompt_name': prompt_names[0] if prompt_names else "",
                'api_tokens': 0,
                'error': str(e),
                'data': '{}'
            }
        finally:
            loop.close()


class URLProcessor:
    """
    Handles URL batch processing with URL-specific company context.
    """
    
    def __init__(self, analyzer: ContentAnalyzer, db_manager: DatabaseManager):
        """Initialize the URL processor."""
        self.analyzer = analyzer
        self.db = db_manager
    
    async def _process_urls_async(self, job_id: str, url_data_list: List[Dict[str, Any]], prompt_names: List[str]):
        """
        Process URLs asynchronously in batches with controlled concurrency.
        """
        total_urls = len(url_data_list)
        processed = 0
        errors = 0
        
        # Determine optimal batch size and concurrency
        batch_size = int(os.environ.get("BATCH_SIZE", "10"))
        max_concurrency = int(os.environ.get("MAX_CONCURRENCY", "5"))
        
        logger.info(f"Processing job {job_id} with {total_urls} URLs in batches of {batch_size} with max concurrency {max_concurrency}")
        
        # Split into batches
        batches = [url_data_list[i:i + batch_size] for i in range(0, len(url_data_list), batch_size)]
        
        # Semaphore to control concurrency
        semaphore = asyncio.Semaphore(max_concurrency)
        
        async def _process_urls_async(self, job_id: str, url_data_list: List[Dict[str, Any]], prompt_names: List[str]):
            """
            Process URLs asynchronously in batches with controlled concurrency.
            """
            total_urls = len(url_data_list)
            processed = 0
            errors = 0
            
            # Determine optimal batch size and concurrency
            batch_size = int(os.environ.get("BATCH_SIZE", "10"))
            max_concurrency = int(os.environ.get("MAX_CONCURRENCY", "5"))
            
            logger.info(f"Processing job {job_id} with {total_urls} URLs in batches of {batch_size} with max concurrency {max_concurrency}")
            
            # Split into batches
            batches = [url_data_list[i:i + batch_size] for i in range(0, len(url_data_list), batch_size)]
            
            # Semaphore to control concurrency
            semaphore = asyncio.Semaphore(max_concurrency)
        
        async def process_url_with_semaphore(url_data):
            """Process a single URL with semaphore control"""
            async with semaphore:
                url = url_data.get('url')
                company_info = url_data.get('company_info')
                content_type = url_data.get('content_type', 'html')
                force_browser = url_data.get('force_browser', False)
                
                try:
                    result = await self.analyzer.process_url_async(
                        url=url,
                        prompt_names=prompt_names,
                        company_info=company_info,
                        content_type=content_type, 
                        force_browser=force_browser
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
                        "created_at": datetime.now().isoformat()
                    }
        
        # Process each batch
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i+1}/{len(batches)} for job {job_id}")
            
            # Create tasks for all URLs in the batch
            tasks = [process_url_with_semaphore(url_data) for url_data in batch if url_data.get('url')]
            
            # Process the batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Save results and update counters
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Exception in URL processing: {str(result)}")
                    errors += 1
                    continue
                
                # Save result to database
                self.db.save_result(result)
                
                # Update counters
                processed += 1
                if result.get('status') != 'success':
                    errors += 1
            
            # Update job status after each batch
            self.db.update_job_status(
                job_id=job_id,
                processed_urls=processed,
                error_count=errors
            )
            
            logger.info(f"Completed batch {i+1}/{len(batches)} for job {job_id} - {processed}/{total_urls} processed, {errors} errors")
        
        # Mark job as completed
        final_status = "completed" if errors == 0 else "completed_with_errors"
        self.db.update_job_status(
            job_id=job_id,
            status=final_status,
            processed_urls=processed,
            error_count=errors,
            completed_at=datetime.now().isoformat()
        )
        
        logger.info(f"Finished job {job_id}: {processed}/{total_urls} URLs processed, {errors} errors")
