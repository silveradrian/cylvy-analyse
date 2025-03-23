from __future__ import annotations  # This must be the first import!

# Standard library imports
import asyncio
import json
import logging
import os
import re
import tempfile
import threading
import time
import random
from bigquery_content_store import BigQueryContentStore
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse

# Third-party imports
import aiohttp
import async_timeout
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from limits import RateLimitItem, storage
from limits.strategies import FixedWindowRateLimiter
from limits.aio.strategies import FixedWindowRateLimiter as AsyncFixedWindowRateLimiter
from openai import OpenAI, AsyncOpenAI
import PyPDF2
from scrapingbee import ScrapingBeeClient
import tiktoken

# Internal imports
from db_manager import DatabaseManager
from prompt_loader import PromptLoader

# Configure logging
logger = logging.getLogger(__name__)

# Initialize OpenAI clients
openai_api_key = os.environ.get("OPENAI_API_KEY", "")
client = OpenAI(api_key=openai_api_key)
async_client = AsyncOpenAI(api_key=openai_api_key)

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
    


    async def extract_document_async(self, url: str, content_type: str) -> Dict[str, Any]:
        """
        Extract text from documents (PDF, DOCX, PPTX) by downloading and processing them.
        
        Args:
            url: URL to the document
            content_type: The type of document ("pdf", "docx", "pptx")
            
        Returns:
            Dictionary containing extraction results
        """
        import tempfile
        import os
        import re
        from urllib.parse import urlparse
        
        start_time = time.time()
        
        # Function to get a filename from URL or headers
        def get_filename_from_url(url, content_disposition=None):
            if content_disposition:
                filename_match = re.findall('filename="(.+)"', content_disposition)
                if filename_match:
                    return filename_match[0]
                    
            # Fallback to URL parsing
            path = urlparse(url).path
            filename = os.path.basename(path)
            return filename if filename else f"document.{content_type}"
        
        logger.info(f"Downloading {content_type.upper()} document: {url}")
        
        try:
            # Random user agent
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'
            ]
            headers = {'User-Agent': random.choice(user_agents)}
            
            # Download the file with timeout (60 seconds)
            response = requests.get(url, headers=headers, timeout=60, stream=True)
            response.raise_for_status()
            
            # Get content disposition for filename
            content_disposition = response.headers.get('Content-Disposition', '')
            filename = get_filename_from_url(url, content_disposition)
            
            # Create a temporary file with the appropriate extension
            with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{content_type}') as temp_file:
                # Download in chunks
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)
                        
                temp_path = temp_file.name
                
            try:
                # Process based on content type
                if content_type == 'pdf':
                    result = self.extract_from_pdf(temp_path)
                elif content_type == 'docx':
                    result = self.extract_from_docx(temp_path)
                elif content_type == 'pptx':
                    result = self.extract_from_pptx(temp_path)
                else:
                    raise ValueError(f"Unsupported document type: {content_type}")
                    
                # Add URL and processing time
                result["url"] = url
                result["scrape_time"] = time.time() - start_time
                
                # If no title was extracted but we have a filename, use it as title
                if not result.get("title") and filename:
                    # Clean up filename to use as title (remove extension, replace underscores)
                    clean_title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')
                    result["title"] = clean_title
                    
                return result
                
            finally:
                # Clean up the temporary file
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    logger.warning(f"Could not delete temporary file: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error extracting document {url}: {str(e)}")
            return {
                "url": url,
                "status": "error",
                "error": str(e),
                "scrape_time": time.time() - start_time
            }

    def extract_from_pdf(self, file_path: str) -> Dict[str, Any]:
        """
        Extract text from a PDF file using multiple extraction methods.
        """
        result = {
            "text": "",
            "title": "",
            "status": "success",
            "content_type": "pdf",
            "method_used": ""
        }
        
        try:
                      
            with open(file_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                
                # Extract metadata
                if reader.metadata:
                    for key, value in reader.metadata.items():
                        if key == "/Title" and value:
                            result["title"] = value
                
                # Extract text from all pages
                all_text = []
                for i in range(len(reader.pages)):
                    page = reader.pages[i]
                    text = page.extract_text()
                    if text:
                        all_text.append(text)
                
                result["text"] = "\n\n".join(all_text)
                result["word_count"] = len(result["text"].split())
                result["method_used"] = "PyPDF2"
                
            # If PyPDF2 didn't extract enough text, try other libraries
            if result["word_count"] < 100:
                try:
                    # Try PyMuPDF (fitz) if available
                    import fitz
                    doc = fitz.open(file_path)
                    
                    # Extract metadata
                    if doc.metadata and "title" in doc.metadata and doc.metadata["title"]:
                        result["title"] = doc.metadata["title"]
                    
                    # Extract text
                    all_text = []
                    for page_num in range(len(doc)):
                        page = doc[page_num]
                        text = page.get_text()
                        all_text.append(text)
                    
                    pymupdf_text = "\n\n".join(all_text)
                    pymupdf_word_count = len(pymupdf_text.split())
                    
                    # Only use if it extracted more text
                    if pymupdf_word_count > result["word_count"]:
                        result["text"] = pymupdf_text
                        result["word_count"] = pymupdf_word_count
                        result["method_used"] = "PyMuPDF"
                    
                except ImportError:
                    logger.info("PyMuPDF not available for PDF extraction")
        
        except ImportError:
            result["status"] = "error"
            result["error"] = "PDF extraction libraries not available"
        except Exception as e:
            result["status"] = "error"
            result["error"] = f"PDF extraction failed: {str(e)}"
        
        # If no text was extracted, mark as error
        if not result["text"]:
            result["status"] = "error" if "error" not in result else result["status"]
            result["error"] = result.get("error", "Failed to extract text from PDF")
        
        return result

    def extract_from_docx(self, file_path: str) -> Dict[str, Any]:
        """
        Extract text from a Word document (.docx file).
        """
        result = {
            "text": "",
            "title": "",
            "status": "success",
            "content_type": "docx",
            "method_used": "python-docx"
        }
        
        try:
            from docx import Document
            
            # Open the document
            doc = Document(file_path)
            
            # Extract title from document properties
            if hasattr(doc, "core_properties"):
                if doc.core_properties.title:
                    result["title"] = doc.core_properties.title
            
            # If no title found, use the first paragraph if it's short enough
            if not result["title"] and doc.paragraphs:
                first_para = doc.paragraphs[0].text.strip()
                if first_para and len(first_para) < 200:
                    result["title"] = first_para
            
            # Extract all text
            paragraphs = []
            for paragraph in doc.paragraphs:
                text = paragraph.text.strip()
                if text:
                    paragraphs.append(text)
            
            # Join paragraphs with newlines
            result["text"] = "\n\n".join(paragraphs)
            result["word_count"] = len(result["text"].split())
            
        except ImportError:
            result["status"] = "error"
            result["error"] = "python-docx package not installed"
        except Exception as e:
            result["status"] = "error"
            result["error"] = f"DOCX extraction failed: {str(e)}"
        
        return result

    def extract_from_pptx(self, file_path: str) -> Dict[str, Any]:
        """
        Extract text from a PowerPoint document (.pptx file).
        """
        result = {
            "text": "",
            "title": "",
            "status": "success",
            "content_type": "pptx",
            "method_used": "python-pptx"
        }
        
        try:
            from pptx import Presentation
            
            # Open the presentation
            prs = Presentation(file_path)
            
            # Try to get title from core properties
            if hasattr(prs, "core_properties") and prs.core_properties.title:
                result["title"] = prs.core_properties.title
            
            # If no title found, use title from first slide if available
            if not result["title"] and len(prs.slides) > 0:
                first_slide = prs.slides[0]
                for shape in first_slide.shapes:
                    if hasattr(shape, "has_text_frame") and shape.has_text_frame:
                        text = shape.text.strip()
                        if text and len(text) < 200:
                            result["title"] = text
                            break
            
            # Extract text from all slides
            all_text = []
            
            for i, slide in enumerate(prs.slides):
                slide_text = []
                slide_text.append(f"Slide {i+1}")
                
                for shape in slide.shapes:
                    if hasattr(shape, "has_text_frame") and shape.has_text_frame:
                        text = shape.text.strip()
                        if text:
                            slide_text.append(text)
                
                if slide_text:
                    all_text.append("\n".join(slide_text))
            
            result["text"] = "\n\n".join(all_text)
            result["word_count"] = len(result["text"].split())
            
        except ImportError:
            result["status"] = "error"
            result["error"] = "python-pptx package not installed"
        except Exception as e:
            result["status"] = "error"
            result["error"] = f"PPTX extraction failed: {str(e)}"
        
        return result



    async def scrape_url_async(self, url: str, content_type: str = "html", 
                    force_browser: bool = False, retry_attempt: int = 0, 
                    timeout: int = 100) -> Dict[str, Any]:
        """
        Asynchronously scrape content from a URL using ScrapingBee's API or document extraction methods.
        Includes automatic retry with enhanced options on failure and request timeout.

        Args:
            url: URL to scrape
            content_type: Expected content type (html, pdf, docx, pptx, or auto)
            force_browser: Whether to force browser rendering for HTML
            retry_attempt: Current retry attempt number
            timeout: Timeout in seconds for the API request (default: 100)
            
        Returns:
            Dictionary containing scraping results
        """
        # Make sure we import re for text normalization
        import re
        import tempfile
        import os
        import requests  # Add this import for document downloads
        from urllib.parse import urlparse

        try:
            # Check if the URL is a document by extension or content_type
            url_lower = url.lower()
            detected_type = content_type.lower()
            
            # Auto-detect content type from URL extension
            if detected_type == "html" or detected_type == "auto":
                if url_lower.endswith(".pdf"):
                    detected_type = "pdf"
                    logger.info(f"Auto-detected PDF document: {url}")
                elif url_lower.endswith(".docx"):
                    detected_type = "docx"
                    logger.info(f"Auto-detected Word document: {url}")
                elif url_lower.endswith(".pptx"):
                    detected_type = "pptx"
                    logger.info(f"Auto-detected PowerPoint document: {url}")
                else:
                    detected_type = "html"  # Default to HTML
            
            # Handle document types (PDF, DOCX, PPTX)
            if detected_type in ["pdf", "docx", "pptx"]:
                logger.info(f"Processing document URL: {url}, type: {detected_type}")
                
                start_time = time.time()
                
                try:
                    # Setup for document download
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    }
                    
                    # Execute the document download with timeout
                    with ThreadPoolExecutor() as executor:
                        loop = asyncio.get_event_loop()
                        
                        async def download_with_timeout():
                            try:
                                async with async_timeout.timeout(timeout):
                                    return await loop.run_in_executor(
                                        executor,
                                        lambda: requests.get(url, headers=headers, timeout=timeout, stream=True)
                                    )
                            except asyncio.TimeoutError:
                                logger.error(f"Timeout after {timeout}s when downloading {url}")
                                raise TimeoutError(f"Document download timed out after {timeout} seconds")
                        
                        try:
                            response = await download_with_timeout()
                        except TimeoutError as e:
                            return {
                                "error": str(e),
                                "url": url,
                                "status": "scrape_error",
                                "content_type": detected_type,
                                "retry_attempts": retry_attempt
                            }
                    
                    # Check if download was successful
                    if response.status_code != 200:
                        return {
                            "error": f"Document download failed with status code: {response.status_code}",
                            "url": url,
                            "status": "scrape_error",
                            "content_type": detected_type,
                            "retry_attempts": retry_attempt
                        }
                    
                    # Create a temporary file with appropriate extension
                    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{detected_type}") as temp_file:
                        temp_path = temp_file.name
                        
                        # Download the file in chunks
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                temp_file.write(chunk)
                    
                    # Process the document based on its type
                    try:
                        if detected_type == "pdf":
                            result = self.extract_from_pdf(temp_path)
                        elif detected_type == "docx":
                            result = self.extract_from_docx(temp_path)
                        elif detected_type == "pptx":
                            result = self.extract_from_pptx(temp_path)
                        
                        # Add common fields
                        result.update({
                            "url": url,
                            "status": "success" if not result.get("error") else "scrape_error",
                            "content_type": detected_type,
                            "scrape_time": time.time() - start_time
                        })
                        
                        # If no title was extracted, try to use filename from URL
                        if not result.get("title"):
                            path = urlparse(url).path
                            filename = os.path.basename(path)
                            if filename:
                                # Clean up the filename to use as title
                                clean_title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')
                                result["title"] = clean_title
                        
                        logger.info(f"Successfully extracted {result.get('word_count', 0)} words from {detected_type.upper()} document: {url}")
                        return result
                        
                    finally:
                        # Clean up temporary file
                        try:
                            if os.path.exists(temp_path):
                                os.unlink(temp_path)
                        except Exception as e:
                            logger.warning(f"Failed to delete temporary file: {str(e)}")
                
                except Exception as e:
                    logger.error(f"Error processing document {url}: {str(e)}")
                    return {
                        "error": f"Document processing error: {str(e)}",
                        "url": url,
                        "status": "scrape_error",
                        "content_type": detected_type,
                        "retry_attempts": retry_attempt
                    }
            
            # Regular HTML scraping with ScrapingBee follows...
            logger.info(f"Async scraping HTML URL with ScrapingBee: {url}{' (retry attempt '+str(retry_attempt)+')' if retry_attempt > 0 else ''}")
            
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
            
            # Determine scraping parameters
            # On retry or if force_browser is True, use enhanced options
            use_enhanced_options = force_browser or retry_attempt > 0
            
            # Set parameters based on retry attempt level
            params = {
                "render_js": "true" if use_enhanced_options else "false",
                "premium_proxy": "true" if use_enhanced_options else "false"
            }
            
            # On second retry, try stealth proxy as a last resort
            if retry_attempt >= 2:
                params["stealth_proxy"] = "true"
                logger.info(f"Using stealth proxy for final retry attempt on {url}")
            
            logger.info(f"Scraping {url} with params: {params}, timeout: {timeout}s")
            
            # Use ThreadPoolExecutor since ScrapingBee client is not async
            with ThreadPoolExecutor() as executor:
                loop = asyncio.get_event_loop()
                
                # Create a function to execute with timeout
                async def fetch_with_timeout():
                    try:
                        # Use async_timeout for the request
                        async with async_timeout.timeout(timeout):
                            return await loop.run_in_executor(
                                executor,
                                lambda: scrapingbee_client.get(url, params=params)
                            )
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout after {timeout}s when scraping {url}")
                        raise TimeoutError(f"ScrapingBee request timed out after {timeout} seconds")
                
                try:
                    response = await fetch_with_timeout()
                except TimeoutError as e:
                    return {
                        "error": str(e),
                        "url": url,
                        "status": "scrape_error",
                        "retry_attempts": retry_attempt
                    }
            
            duration = time.time() - start_time
            
            # Check response status
            if response.status_code != 200:
                # Create a sanitized error message without HTML
                clean_error = self._sanitize_error_response(response)
                error_message = f"ScrapingBee error: {response.status_code} - {clean_error}"
                logger.error(error_message)
                
                # Check if this is an error that would benefit from a retry with enhanced options
                if retry_attempt < 2 and not (params.get("render_js") == "true" and params.get("premium_proxy") == "true"):
                    retry_error_indicators = [
                        "403", "500", "429", "404",  # Status codes
                        "try with render_js=True", 
                        "try with premium_proxy=True",
                        "blocked", "captcha", "cloudflare",
                        "Error with your request"
                    ]
                    
                    should_retry = any(indicator in str(response.status_code) for indicator in ["403", "500", "429", "404"])
                    
                    response_text = clean_error
                    if not should_retry and response_text:
                        should_retry = any(indicator.lower() in response_text.lower() for indicator in retry_error_indicators)
                    
                    if should_retry:
                        logger.warning(f"Retrying {url} with enhanced options (attempt {retry_attempt+1})")
                        await asyncio.sleep(1 * (retry_attempt + 1))  # Progressive delay
                        return await self.scrape_url_async(url, content_type, True, retry_attempt + 1, timeout)
                
                # Return the error if we've exhausted retries or enhanced options didn't help
                return {
                    "error": error_message,
                    "url": url,
                    "status": "scrape_error",
                    "retry_attempts": retry_attempt
                }
            
            # Process HTML content
            html_content = response.content.decode('utf-8', errors='ignore')
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract title
            title = ""
            if soup.title:
                title = soup.title.text.strip()
            
            # IMPROVED CONTENT EXTRACTION:
            
            # 1. Remove unwanted elements more aggressively
            for selector in [
                "script", "style", "noscript", "iframe", "head", 
                "footer", "nav"
            ]:
                for element in soup.find_all(selector):
                    element.extract()
                    
            # Also try to remove common non-content elements by class/id
            for selector in [
                "[role=banner]", "[role=navigation]", "[role=complementary]", "[role=contentinfo]",
                ".cookie-banner", ".cookie-consent", ".newsletter-signup",
                ".ad", ".advertisement", ".sidebar", "header"
            ]:
                try:
                    for element in soup.select(selector):
                        element.extract()
                except:
                    pass  # Ignore errors with advanced selectors
            
            # 2. Try to find the main content using common content selectors
            main_content_selectors = [
                "main", "article", "[role=main]", "#content", ".content", 
                "#main-content", ".main-content", ".post-content",
                ".entry-content", "[itemprop=articleBody]"
            ]
            
            main_content = None
            found_selector = None
            for selector in main_content_selectors:
                try:
                    elements = soup.select(selector)
                    if elements:
                        # Use the largest matching element (by text length)
                        main_content = max(elements, key=lambda e: len(e.get_text()))
                        found_selector = selector
                        break
                except:
                    continue  # Skip if selector syntax error
            
            # 3. Extract text from the main content if found, otherwise from the whole page
            if main_content:
                text = main_content.get_text(separator="\n", strip=True)
                logger.info(f"Found main content using selector: {found_selector}")
            else:
                # Fall back to the whole page but with smarter text extraction
                logger.info(f"No main content container found, extracting from whole page")
                
                # Get all text nodes with some minimum length to avoid menu items and buttons
                paragraphs = []
                
                # Try to find paragraph-like elements or text blocks
                for p in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'div']):
                    p_text = p.get_text(strip=True)
                    # Only include paragraphs with reasonable content (ignore short menu items, etc.)
                    if len(p_text) > 30:  # Minimum paragraph length
                        paragraphs.append(p.get_text(separator=" ", strip=True))
                
                if paragraphs:
                    text = "\n\n".join(paragraphs)
                else:
                    # Last resort: just get all text
                    text = soup.get_text(separator="\n", strip=True)
            
            # 4. Remove excessive whitespace and normalize
            text = re.sub(r'\s+', ' ', text).strip()
            text = re.sub(r'\n\s*\n', '\n\n', text)
            
            # Count words
            word_count = len(text.split())
            
            # Log content extraction results
            logger.info(f"Extracted {word_count} words from {url}")
            
            # Check if the content seems too short (likely blocked or empty page)
            if word_count < 50 and retry_attempt < 2 and not use_enhanced_options:
                logger.warning(f"Content for {url} seems too short ({word_count} words). Retrying with enhanced options.")
                await asyncio.sleep(1)
                return await self.scrape_url_async(url, content_type, True, retry_attempt + 1, timeout)
            
            result = {
                "url": url,
                "title": title,
                "text": text,
                "word_count": word_count,
                "status": "success",
                "content_type": "html",
                "scrape_time": duration,
                "enhanced_scraping": use_enhanced_options,
                "retry_attempts": retry_attempt
            }
            
            logger.info(f"Successfully scraped {url} - {result.get('word_count', 0)} words in {duration:.2f}s")
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"Async timeout after {timeout}s when scraping {url}")
            return {
                "error": f"Request timed out after {timeout} seconds",
                "url": url,
                "status": "scrape_error",
                "retry_attempts": retry_attempt
            }
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            
            # If this is not yet a retry and we have a connection error, try again with enhanced options
            connection_errors = ["connection", "timeout", "too many requests", "reset by peer"]
            if retry_attempt < 2 and not force_browser and any(err in str(e).lower() for err in connection_errors):
                logger.warning(f"Connection error for {url}. Retrying with enhanced options after delay.")
                await asyncio.sleep(2 * (retry_attempt + 1))  # Progressive delay for connection issues
                return await self.scrape_url_async(url, content_type, True, retry_attempt + 1, timeout)
                
            return {
                "error": str(e),
                "url": url, 
                "status": "scrape_error",
                "retry_attempts": retry_attempt
            }

    def _sanitize_error_response(self, response):
        """
        Sanitize error responses to avoid storing HTML in the database or showing it in exports.
        
        Args:
            response: The response object from ScrapingBee
            
        Returns:
            A cleaned error message without HTML
        """
        import re
        try:
            # Try to parse as JSON first
            if response.text and response.text.strip().startswith('{'):
                error_info = json.loads(response.text)
                # Extract useful fields from the error JSON
                error_reason = error_info.get('reason', '')
                error_help = error_info.get('help', '')
                if error_reason and error_help:
                    return f"Reason: {error_reason}. Help: {error_help}"
                elif error_reason:
                    return f"Reason: {error_reason}"
                else:
                    return "Unknown error (JSON response with no reason field)"
            
            # If it contains HTML, try to extract useful information
            if "<html" in response.text.lower():
                try:
                    # Try to extract title
                    soup = BeautifulSoup(response.text, 'html.parser')
                    title = soup.title.text.strip() if soup.title else None
                    if title:
                        return f"HTML response - Title: {title}"
                    else:
                        # If no title, just indicate it was HTML and the size
                        size_kb = len(response.text) / 1024
                        return f"HTML response ({size_kb:.1f}KB)"
                except:
                    return "HTML response (parsing failed)"
            
            # If not HTML or JSON, return a short preview
            if len(response.text) > 100:
                return response.text[:100].replace('\n', ' ') + "..."
            
            # If it's short enough, return as is
            return response.text.replace('\n', ' ')
            
        except Exception as e:
            # If anything fails in the sanitization, return a safe message
            return f"Error response (sanitization failed: {str(e)})"
    

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
                          force_browser: bool = False,
                          job_id: str = None) -> Dict[str, Any]:
    """
    Asynchronously process a URL by scraping content and analyzing it.
    
    Args:
        url: URL to process
        prompt_names: List of prompt configuration names to use
        company_info: Optional company context info
        content_type: Content type (html, pdf, etc.)
        force_browser: Whether to force browser-based scraping
        job_id: Optional job ID for tracking related requests
        
    Returns:
        Dictionary containing processing results
    """
    # Initialize BigQueryContentStore if not already done
    if not hasattr(self, 'content_store'):
        try:
            from bigquery_content_store import BigQueryContentStore
            self.content_store = BigQueryContentStore()
        except ImportError:
            logger.warning("BigQueryContentStore not available - BigQuery integration disabled")
            self.content_store = None
    
    logger.info(f"Async processing URL: {url} with {len(prompt_names)} prompts")
    
    # Load prompts
    prompt_loader = PromptLoader()
    prompt_configs = prompt_loader.load_prompts()
    
    result = {
        "url": url,
        "status": "pending",
        "job_id": job_id,
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
        
        # Store failed scrape in BigQuery if enabled
        if hasattr(self, 'content_store') and self.content_store and self.content_store.enable_storage:
            try:
                await self.content_store.store_content(
                    url=url,
                    title=content_result.get("title", ""),
                    content_text="",  # No content for failed scrapes
                    job_id=job_id,
                    content_type=content_type,
                    scrape_info={
                        "status": "error",
                        "error": content_result.get("error", "Unknown scraping error"),
                        "scrape_time": time.time() - start_time
                    },
                    company_info=company_info
                )
            except Exception as e:
                logger.error(f"Error storing failed scrape in BigQuery: {str(e)}")
        
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
    structured_data = {}
    
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
            
            # Extract structured data from analysis text with delimiter format
            analysis_text = analysis_result.get("analysis", "")
            extracted_fields = {}
            
            # Look for field||| or field ||| patterns in the text
            for line in analysis_text.split('\n'):
                if '|||' in line:
                    parts = line.split('|||', 1)  # Split on first occurrence only
                    if len(parts) == 2:
                        field_name = parts[0].strip()
                        field_value = parts[1].strip()
                        
                        if field_name and field_value:
                            extracted_fields[field_name] = field_value
            
            # Add the analysis result with parsed fields
            result["analysis_results"][prompt_name] = {
                "analysis": analysis_result.get("analysis", ""),
                "model": analysis_result.get("model", ""),
                "tokens": analysis_result.get("total_tokens", 0),
                "processing_time": analysis_result.get("processing_time", 0),
                "parsed_fields": extracted_fields  # Add extracted fields
            }
            
            # If we found structured fields, add them to the structured data
            if extracted_fields:
                if prompt_name not in structured_data:
                    structured_data[prompt_name] = {}
                structured_data[prompt_name].update(extracted_fields)
            
            # Update token count
            api_tokens += analysis_result.get("total_tokens", 0)
            
            logger.info(f"Successfully analyzed content with prompt '{prompt_name}'")
            
        except Exception as e:
            logger.error(f"Error analyzing with prompt '{prompt_name}': {str(e)}")
            result["analysis_results"][prompt_name] = {
                "error": str(e)
            }
    
    # Add structured data to result
    if structured_data:
        result["structured_data"] = structured_data
        
        # Also add structured data to result root for backwards compatibility
        for prompt_fields in structured_data.values():
            for field_name, field_value in prompt_fields.items():
                result[field_name] = field_value
    
    # Update final result
    total_time = time.time() - start_time
    result.update({
        "status": "success",
        "api_tokens": api_tokens,
        "analysis_time": total_time - result["scrape_time"],
        "total_time": total_time
    })
    
    # Store the scraped content and analysis results in BigQuery
    if hasattr(self, 'content_store') and self.content_store and self.content_store.enable_storage:
        try:
            # Prepare analysis info
            analysis_info = {
                "api_tokens": api_tokens,
                "analysis_time": result["analysis_time"],
                "prompt_names": prompt_names,
                "analysis_count": len(prompt_names)
            }
            
            # Store content in BigQuery
            content_id = await self.content_store.store_content(
                url=url,
                title=result.get("title", ""),
                content_text=content_text,
                job_id=job_id,
                content_type=result.get("content_type", "html"),
                scrape_info={
                    "status": "success",
                    "scrape_time": result["scrape_time"],
                    "enhanced_scraping": content_result.get("enhanced_scraping", False),
                    "retry_attempts": content_result.get("retry_attempts", 0)
                },
                company_info=company_info,
                analysis_info=analysis_info
            )
            
            # Add content ID reference to result
            if content_id:
                result["content_id"] = content_id
                logger.info(f"Stored content in BigQuery with ID: {content_id}")
        except Exception as e:
            logger.error(f"Error storing content in BigQuery: {str(e)}")
    
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
            user_message = user_message_template.format(
                content=content[:50000],  # Limit content length
                company_context=company_context
            )
            
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
            
            # Log successful processing
            logger.info(f"Processed URL {url} with {len(prompt_names)} prompts - Status: {result['status']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in process_url for {url}: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
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
            # Properly close the loop and clean up resources
            try:
                self.close_async_connections(loop)
            except Exception as e:
                logger.warning(f"Error during async cleanup for {url}: {str(e)}")
            
            try:
                loop.close()
            except Exception as e:
                logger.warning(f"Error closing event loop for {url}: {str(e)}")
    
            
    def close_async_connections(self, loop=None):
        """Properly close async connections to prevent event loop errors."""
        import asyncio
        import concurrent.futures
        
        try:
            # Get the current event loop if none is provided
            if loop is None:
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    # If no event loop exists, there's nothing to clean up
                    return
            
            # Get all tasks in the loop
            pending = asyncio.all_tasks(loop)
            
            # Create a task to gracefully cancel all pending tasks
            if pending:
                # Create a future to wait for the cleanup
                cleanup_future = asyncio.run_coroutine_threadsafe(
                    asyncio.gather(*pending, return_exceptions=True),
                    loop
                )
                try:
                    # Wait for a short timeout
                    cleanup_future.result(timeout=1.0)
                except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
                    logger.warning("Timed out waiting for task cleanup")
        except Exception as e:
            logger.warning(f"Error during async cleanup: {e}")


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
