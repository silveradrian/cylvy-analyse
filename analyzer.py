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
logger = logging.getLogger(__name__)

# Initialize OpenAI clients
openai_api_key = os.environ.get("OPENAI_API_KEY", "")
client = OpenAI(api_key=openai_api_key)
async_client = AsyncOpenAI(api_key=openai_api_key)

class AdvancedRateLimiter:
    """Advanced rate limiter using limits library"""
    
    def __init__(self, rate_limit_per_minute: int = 60):
        self.rate_limit_per_minute = rate_limit_per_minute
        self.storage = storage.MemoryStorage()
        self.limiter = FixedWindowRateLimiter(self.storage)
        # The issue is with this line - RateLimitItem constructor takes different arguments
        # self.item = RateLimitItem("api_calls", "api_calls_per_minute", rate_limit_per_minute, 60)
        logger.info(f"Advanced rate limiter initialized: {rate_limit_per_minute} requests/minute")
    
    def __enter__(self):
        """Wait until a request is allowed"""
        # Direct use of the limiter without RateLimitItem
        while not self.limiter.hit("api_calls", self.rate_limit_per_minute, 60):
            time.sleep(0.1)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class AsyncAdvancedRateLimiter:
    """Async advanced rate limiter for high throughput operations"""
    
    def __init__(self, rate_limit_per_minute: int = 60):
        self.rate_limit_per_second = rate_limit_per_minute / 60.0
        self.limiter = AsyncLimiter(self.rate_limit_per_second, 1)
        logger.info(f"Async rate limiter initialized: {rate_limit_per_minute} requests/minute")
    
    async def __aenter__(self):
        await self.limiter.acquire()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class ContentAnalyzer:
    """
    Analyzes content from URLs using OpenAI's API.
    """
    
    def __init__(self):
        """Initialize the content analyzer."""
        self.openai_client = client
        self.async_openai_client = async_client
        
        # Get API key
        scrapingbee_api_key = os.environ.get("SCRAPINGBEE_API_KEY")
        if not scrapingbee_api_key:
            logger.warning("SCRAPINGBEE_API_KEY not found in environment variables!")
        
        # Create client with absolute minimum code
        self.scrapingbee_client = ScrapingBeeClient(api_key=scrapingbee_api_key)
        
        # Set up rate limiters for different API calls
        openai_rate_limit = int(os.environ.get("OPENAI_RATE_LIMIT", "60"))
        scrapingbee_rate_limit = int(os.environ.get("SCRAPINGBEE_RATE_LIMIT", "50"))
        
        self.openai_rate_limiter = AdvancedRateLimiter(openai_rate_limit)
        self.async_openai_rate_limiter = AsyncAdvancedRateLimiter(openai_rate_limit)
        self.scrapingbee_rate_limiter = AdvancedRateLimiter(scrapingbee_rate_limit)
        self.async_scrapingbee_rate_limiter = AsyncAdvancedRateLimiter(scrapingbee_rate_limit)
        
        logger.info(f"Content analyzer initialized with advanced rate limiters")
        logger.info(f"OpenAI rate limit: {openai_rate_limit} requests/minute")
        logger.info(f"ScrapingBee rate limit: {scrapingbee_rate_limit} requests/minute")
    
    async def scrape_url_async(self, url: str, content_type: str = "html", 
                        force_browser: bool = False) -> Dict[str, Any]:
        """
        Asynchronously scrape content from a URL using ScrapingBee's API.
        
        Args:
            url: URL to scrape
            content_type: Content type (html, pdf, etc.)
            force_browser: Whether to force browser-based scraping
            
        Returns:
            Dictionary containing the scraped content
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
            
            start_time = time.time()
            
            # Use the async rate limiter for ScrapingBee
            async with self.async_scrapingbee_rate_limiter:
                # Since ScrapingBee doesn't have a native async client,
                # we'll use a ThreadPoolExecutor to avoid blocking
                with ThreadPoolExecutor() as executor:
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        executor,
                        lambda: self.scrapingbee_client.get(url)
                    )
            
            duration = time.time() - start_time
            
            # Check response status
            if response.status_code != 200:
                logger.error(f"ScrapingBee error: {response.status_code} - {response.text}")
                return {
                    "error": f"ScrapingBee returned status code {response.status_code}",
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

    async def scrape_urls_in_batch(self, urls: List[str], content_type: str = "html",
                                  force_browser: bool = False) -> List[Dict[str, Any]]:
        """
        Scrape multiple URLs in parallel with controlled concurrency.
        
        Args:
            urls: List of URLs to scrape
            content_type: Content type (html, pdf, etc.)
            force_browser: Whether to force browser-based scraping
            
        Returns:
            List of dictionaries containing the scraped content
        """
        # Determine appropriate concurrency
        # Default to 5, but allow 20 for premium if specified
        max_concurrency = int(os.environ.get("SCRAPINGBEE_CONCURRENCY", "5"))
        logger.info(f"Batch scraping {len(urls)} URLs with concurrency {max_concurrency}")
        
        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(max_concurrency)
        
        async def scrape_with_semaphore(url):
            async with semaphore:
                return await self.scrape_url_async(url, content_type, force_browser)
        
        # Create tasks for all URLs
        tasks = [scrape_with_semaphore(url) for url in urls]
        
        # Execute all tasks and gather results
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results, handling any exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error scraping {urls[i]}: {str(result)}")
                processed_results.append({
                    "error": str(result),
                    "url": urls[i],
                    "status": "error"
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def analyze_with_prompt_async(self, content: str, prompt_config: Dict[str, Any], 
                                      company_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Asynchronously analyze content using OpenAI model with a specific prompt configuration.
        
        Args:
            content: Content to analyze
            prompt_config: Prompt configuration
            company_info: Optional company context info
            
        Returns:
            Dictionary containing analysis results and metadata
        """
        try:
            start_time = time.time()
            
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
            user_message = user_message_template.format(
                content=content[:50000],  # Limit content length
                company_context=company_context
            )
            
            # Log the request details
            logger.info(f"Calling OpenAI API with model {model} - content length: {len(content)} chars")
            
            # Use the async rate limiter
            async with self.async_openai_rate_limiter:
                # Call the OpenAI API asynchronously
                response = await self.async_openai_client.chat.completions.create(
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
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            logger.info(f"OpenAI API call completed in {processing_time:.2f}s")
            logger.info(f"Token usage: {total_tokens} tokens")
            
            return {
                "analysis": analysis_text,
                "model": model,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "processing_time": processing_time
            }
            
        except Exception as e:
            logger.error(f"Error in async OpenAI API call: {str(e)}")
            return {
                "error": str(e),
                "analysis": "Error: Could not complete analysis."
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
    
    async def process_url_async(self, url: str, prompt_names: List[str], 
                             company_info: Optional[Dict[str, Any]] = None,
                             content_type: str = "html", 
                             force_browser: bool = False) -> Dict[str, Any]:
        """
        Asynchronously process a URL by scraping content and analyzing it.
        
        Args:
            url: URL to process
            prompt_names: List of prompt configuration names to use
            company_info: Optional company context info
            content_type: Content type (html, pdf, etc.)
            force_browser: Whether to force browser-based scraping
            
        Returns:
            Dictionary containing processing results
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
            "analysis_results": {}
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
                    "processing_time": analysis_result.get("processing_time", 0)
                }
                
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
    
    def analyze_with_prompt(self, content: str, prompt_config: Dict[str, Any], 
                         company_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Synchronous wrapper for analyze_with_prompt_async.
        """
        try:
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
            user_message = user_message_template.format(
                content=content[:50000],  # Limit content length
                company_context=company_context
            )
            
            # Log the request details
            logger.info(f"Calling OpenAI API with model {model} - content length: {len(content)} chars")
            
            # Use the rate limiter
            with self.openai_rate_limiter:
                # Call the OpenAI API
                response = self.openai_client.chat.completions.create(
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
            
            logger.info(f"Token usage: {total_tokens} tokens")
            
            return {
                "analysis": analysis_text,
                "model": model,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens
            }
            
        except Exception as e:
            logger.error(f"Error in OpenAI API call: {str(e)}")
            return {
                "error": str(e),
                "analysis": "Error: Could not complete analysis."
            }
    
    def process_url(self, url: str, prompt_names: List[str], 
             company_info: Optional[Dict[str, Any]] = None,
             content_type: str = "html", 
             force_browser: bool = False) -> Dict[str, Any]:
        """
        Process a single URL by scraping content and analyzing it with selected prompts.
        
        Args:
            url: URL to process
            prompt_names: List of prompt configuration names to use
            company_info: Optional company context info
            content_type: Content type (html, pdf, etc.)
            force_browser: Whether to force browser-based scraping
            
        Returns:
            Dictionary containing processing results formatted for database storage
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
            
            # Format result to match database schema (results table)
            result = {
                'url': url,
                'status': raw_result.get('status', 'unknown'),
                'title': raw_result.get('title', ''),
                'content_type': raw_result.get('content_type', 'html'),
                'word_count': raw_result.get('word_count', 0),
                'processed_at': current_time,  # Use processed_at instead of created_at
                'prompt_name': prompt_name,    # Use first prompt as primary prompt
                'api_tokens': api_tokens,
                'error': raw_result.get('error', ''),
                'data': json.dumps(raw_result.get('analysis_results', {}))
            }
            
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
                'processed_at': time.time(),  # Current time
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
    
    def process_url_batch(self, job_id: str, url_data_list: List[Dict[str, Any]], prompt_names: List[str]):
        """
        Process a batch of URLs with their specific company contexts.
        Wrapper for the async version.
        
        Args:
            job_id: The job identifier
            url_data_list: List of dictionaries containing URL and its associated company context
                Each dictionary should have at least a 'url' key and optionally a 'company_info' key
            prompt_names: List of prompt configuration names to use
        """
        # Update job status to running
        self.db.update_job_status(
            job_id=job_id,
            status="running",
            total_urls=len(url_data_list),
            processed_urls=0,
            error_count=0
        )
        
        # Run the async version in a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                self._process_urls_async(job_id, url_data_list, prompt_names)
            )
        finally:
            loop.close()
