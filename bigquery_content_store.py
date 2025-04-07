import os
import json
import logging
import asyncio

from datetime import datetime

from typing import Dict, Any, List, Optional, Tuple

# Configure logging
logger = logging.getLogger(__name__)

# Add custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects by converting them to ISO format strings."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

# Configure logging
logger = logging.getLogger(__name__)

# Add custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects by converting them to ISO format strings."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

class BigQueryContentStore:
    """
    Handles storage and retrieval of raw scraped content in Google BigQuery.
    Focused on content storage only (no vector embeddings).
    """
    def __init__(self, dataset_id=None, project_id=None, auto_initialize=True):
        """Initialize BigQuery content store."""
        # Explicitly set these to their default values if env vars are not provided
        self.project_id = project_id or os.environ.get("GOOGLE_CLOUD_PROJECT", "cylvy-analyse")
        self.dataset_id = dataset_id or os.environ.get("BIGQUERY_DATASET_ID", "content_store")
        
        # Authentication status tracking
        self._authenticated = False
        self._auth_error = None
        self._auth_checked = False
        
        # Default to enabled, will be set to False if auth fails
        self.enable_storage = True
        
        # Log configuration for debugging
        logger.info(f"BigQuery Config - Project: {self.project_id}, Dataset: {self.dataset_id}")
        logger.info(f"Raw Storage Enabled: {self.enable_storage}")
        
        # Initialize BigQuery client if requested (default)
        # But don't fail if it doesn't work - we'll track the error
        self.client = None
        if auto_initialize:
            try:
                self._lazy_init()
            except Exception as e:
                logger.warning(f"BigQuery initialization deferred: {str(e)}")
    
    def _lazy_init(self):
        """Lazy initialization of BigQuery client."""
        if self.client is not None:
            return
            
        # Check if already disabled
        if not self.enable_storage:
            return
            
        try:
            self._initialize_bigquery()
        except Exception as e:
            # Don't raise, just log and track error
            logger.warning(f"BigQuery initialization failed: {str(e)}")
            self._auth_error = str(e)
    
    def _initialize_bigquery(self):
        """Initialize BigQuery client and create tables if needed."""
        try:
            from google.cloud import bigquery
            
            # Check for project
            if not self.project_id:
                logger.error("Google Cloud project ID not specified")
                self._auth_error = "Project ID is required for BigQuery"
                self._authenticated = False
                self.enable_storage = False
                raise ValueError("Project ID is required for BigQuery")
                
            if not self.dataset_id:
                logger.error("BigQuery dataset ID not specified")
                self._auth_error = "Dataset ID is required for BigQuery"
                self._authenticated = False
                self.enable_storage = False
                raise ValueError("Dataset ID is required for BigQuery")
            
            # Create BigQuery client
            logger.info(f"Creating BigQuery client for project {self.project_id}")
            self.client = bigquery.Client(project=self.project_id)
            
            # Check if dataset exists, create if not
            dataset_ref = self.client.dataset(self.dataset_id)
            try:
                self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {self.dataset_id} already exists")
            except Exception as e:
                # Create dataset
                logger.info(f"Dataset {self.dataset_id} not found, creating: {str(e)}")
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = os.environ.get("BIGQUERY_LOCATION", "US")
                dataset = self.client.create_dataset(dataset)
                logger.info(f"Created dataset {self.dataset_id}")
            
            # Create content table if it doesn't exist
            self._create_content_table()
            
            # Authentication successful
            self._authenticated = True
            self._auth_error = None
            self._auth_checked = True
                
        except ImportError:
            error_msg = "Google Cloud BigQuery library not installed. Run: pip install google-cloud-bigquery"
            logger.error(error_msg)
            self._auth_error = error_msg
            self._authenticated = False
            self._auth_checked = True
            self.enable_storage = False
        except Exception as e:
            logger.error(f"Error initializing BigQuery: {str(e)}")
            self._auth_error = str(e)
            self._authenticated = False
            self._auth_checked = True
            self.enable_storage = False
    
    def _create_content_table(self):
        """Create the raw content table if it doesn't exist."""
        from google.cloud import bigquery
        
        table_id = f"{self.project_id}.{self.dataset_id}.raw_content"
        logger.info(f"Checking for table: {table_id}")
        
        # Schema with fields for content, job details, and URL analysis
        schema = [
            bigquery.SchemaField("content_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_id", "STRING"),
            bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("content_text", "STRING"),
            bigquery.SchemaField("content_type", "STRING"),
            bigquery.SchemaField("word_count", "INTEGER"),
            bigquery.SchemaField("domain", "STRING"),
            bigquery.SchemaField("scrape_status", "STRING"),
            bigquery.SchemaField("scrape_time", "FLOAT"),
            bigquery.SchemaField("analysis_tokens", "INTEGER"),
            bigquery.SchemaField("metadata", "JSON"),
            bigquery.SchemaField("company_info", "JSON"),
            bigquery.SchemaField("created_at", "TIMESTAMP")
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            # Check if table exists
            self.client.get_table(table)
            logger.info(f"Content table {table_id} already exists")
        except Exception:
            # Create table
            table = self.client.create_table(table)
            logger.info(f"Created content table {table_id}")
    
    async def store_content(self, url: str, title: str, content_text: str, 
                           job_id: Optional[str] = None, content_type: str = "html", 
                           scrape_info: Dict[str, Any] = None, 
                           company_info: Optional[Dict[str, Any]] = None,
                           analysis_info: Optional[Dict[str, Any]] = None,
                           metadata: Optional[Dict[str, Any]] = None,
                           structured_data: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        Store scraped content in BigQuery database.
        
        Args:
            url: Source URL of content
            title: Title of the content
            content_text: Raw text content
            job_id: The job ID that processed this content (optional)
            content_type: Type of content (html, pdf, docx, etc.)
            scrape_info: Information about the scraping process
            company_info: Company context information
            analysis_info: Information about the analysis (tokens, model, etc.)
            metadata: Additional metadata about the content
            structured_data: Structured data extracted from the content
            
        Returns:
            content_id: Unique ID for the stored content
        """
        # Make sure we're initialized
        if self.client is None:
            try:
                self._lazy_init()
            except Exception as e:
                logger.warning(f"Failed to initialize BigQuery during store_content: {str(e)}")
                
        # Check if storage is enabled
        if not self.enable_storage:
            logger.warning(f"Storage disabled, not storing content for: {url}")
            return None
            
        # Check authentication if not checked yet
        if not self._auth_checked:
            authenticated = await self.check_authentication()
            if not authenticated:
                logger.warning(f"BigQuery authentication failed, not storing content for: {url}")
                return None
            
        logger.info(f"Preparing to store content for URL: {url}")
            
        # Generate a unique content ID
        import uuid
        content_id = str(uuid.uuid4())
        
        # Parse domain from URL
        from urllib.parse import urlparse
        domain = ""
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
        except:
            pass
        
        # Prepare scrape info
        scrape_info = scrape_info or {}
        scrape_status = scrape_info.get("status", "success")
        scrape_time = scrape_info.get("scrape_time", 0.0)
        
        # Calculate word count
        word_count = len(content_text.split()) if content_text else 0
        
        # Extract analysis tokens if available
        analysis_tokens = 0
        if analysis_info:
            analysis_tokens = analysis_info.get("api_tokens", 0)
        
        # Create metadata if not provided
        metadata = metadata or {}
        
        # Add additional fields to metadata
        metadata.update({
            "url": url,
            "content_type": content_type,
            "word_count": word_count,
            "char_count": len(content_text) if content_text else 0,
            "domain": domain,
            "job_id": job_id,
            "stored_at": datetime.now().isoformat()
        })
        
        # Store any scrape details in metadata
        if scrape_info:
            metadata["scrape_details"] = scrape_info
            
        # Store any analysis details in metadata
        if analysis_info:
            metadata["analysis_details"] = analysis_info
            
        # Store structured data in metadata
        if structured_data:
            metadata["structured_data"] = structured_data
        
        # Store raw content
        content_id = await self._store_raw_content(
            content_id=content_id,
            job_id=job_id,
            url=url,
            title=title,
            content_text=content_text, 
            content_type=content_type,
            word_count=word_count,
            domain=domain,
            scrape_status=scrape_status,
            scrape_time=scrape_time,
            analysis_tokens=analysis_tokens,
            metadata=metadata,
            company_info=company_info
        )
            
        return content_id
    
    async def _store_raw_content(self, content_id, job_id, url, title, content_text, 
                               content_type, word_count, domain, scrape_status, 
                               scrape_time, analysis_tokens, metadata, company_info):
        """Store raw content in BigQuery."""
        try:
            from google.cloud import bigquery
            
            # Get current time for timestamp
            current_time = datetime.now()
            
            # Enhanced logging for debugging
            logger.info(f"BIGQUERY INSERT - job_id value: '{job_id}'")
            if company_info:
                logger.info(f"BIGQUERY INSERT - company_info present: {type(company_info)}, keys: {list(company_info.keys()) if isinstance(company_info, dict) else 'Not a dict'}")
            else:
                logger.info(f"BIGQUERY INSERT - company_info is None or empty")
            
            # Prepare row data - using custom JSON encoder for datetime objects
            row = {
                "content_id": content_id,
                "job_id": job_id,
                "url": url,
                "title": title,
                "content_text": content_text,
                "content_type": content_type,
                "word_count": word_count,
                "domain": domain,
                "scrape_status": scrape_status,
                "scrape_time": scrape_time,
                "analysis_tokens": analysis_tokens,
                "metadata": json.dumps(metadata, cls=DateTimeEncoder),
                "company_info": json.dumps(company_info, cls=DateTimeEncoder) if company_info else None,
                "created_at": current_time.isoformat()
            }
            
            # Insert row
            table_id = f"{self.project_id}.{self.dataset_id}.raw_content"
            logger.info(f"Inserting row into table: {table_id}")
            errors = self.client.insert_rows_json(table_id, [row])
            
            if errors:
                logger.error(f"Errors inserting content into BigQuery: {errors}")
                return None
            else:
                logger.info(f"Stored raw content for {url} (ID: {content_id}, Job ID: {job_id})")
                return content_id
                
        except Exception as e:
            logger.error(f"Error storing content in BigQuery: {str(e)}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            
            # Check if it's an authentication error
            error_msg = str(e).lower()
            if "authentication" in error_msg or "credential" in error_msg or "permission" in error_msg:
                self._authenticated = False
                self._auth_error = str(e)
                self.enable_storage = False
            
            return None
    
    async def get_content_by_job_id(self, job_id, limit=100, offset=0):
        """
        Retrieve all content for a specific job ID.
        
        Args:
            job_id: The job ID to query
            limit: Maximum number of results
            offset: Starting offset for pagination
            
        Returns:
            List of content items for the job
        """
        # Make sure we're initialized
        if self.client is None:
            try:
                self._lazy_init()
            except Exception:
                pass
                
        if not self.enable_storage:
            logger.warning("Content storage is not enabled")
            return []
            
        try:
            from google.cloud import bigquery
            
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.raw_content`
            WHERE job_id = '{job_id}'
            ORDER BY created_at DESC
            LIMIT {limit}
            OFFSET {offset}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            content_items = []
            for row in results:
                metadata = json.loads(row.metadata) if row.metadata else {}
                company_info = json.loads(row.company_info) if row.company_info else {}
                
                # Truncate content text for result list
                display_text = row.content_text[:500] + "..." if row.content_text and len(row.content_text) > 500 else row.content_text
                
                content_items.append({
                    "content_id": row.content_id,
                    "job_id": row.job_id,
                    "url": row.url,
                    "title": row.title,
                    "content_text": display_text,
                    "content_type": row.content_type,
                    "word_count": row.word_count,
                    "domain": row.domain,
                    "scrape_status": row.scrape_status,
                    "scrape_time": row.scrape_time,
                    "analysis_tokens": row.analysis_tokens,
                    "metadata": metadata,
                    "company_info": company_info,
                    "created_at": row.created_at.isoformat() if row.created_at else None
                })
                
            return content_items
            
        except Exception as e:
            logger.error(f"Error retrieving content by job ID: {str(e)}")
            return []

    async def get_job_stats(self, job_id):
        """
        Get statistics for a specific job ID.
        
        Args:
            job_id: The job ID to query
            
        Returns:
            Dictionary of job statistics
        """
        # Make sure we're initialized
        if self.client is None:
            try:
                self._lazy_init()
            except Exception:
                pass
                
        if not self.enable_storage:
            logger.warning("Content storage is not enabled")
            return {}
            
        try:
            from google.cloud import bigquery
            
            query = f"""
            SELECT
                COUNT(*) as total_documents,
                SUM(word_count) as total_words,
                AVG(word_count) as avg_words_per_doc,
                SUM(analysis_tokens) as total_tokens,
                COUNT(DISTINCT domain) as unique_domains,
                COUNT(CASE WHEN scrape_status = 'success' THEN 1 END) as successful_scrapes,
                COUNT(CASE WHEN scrape_status != 'success' THEN 1 END) as failed_scrapes,
                MIN(created_at) as first_document_time,
                MAX(created_at) as last_document_time
            FROM `{self.project_id}.{self.dataset_id}.raw_content`
            WHERE job_id = '{job_id}'
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                # Calculate duration if both timestamps exist
                duration_minutes = 0
                if row.first_document_time and row.last_document_time:
                    duration_seconds = (row.last_document_time - row.first_document_time).total_seconds()
                    duration_minutes = duration_seconds / 60
                
                return {
                    "job_id": job_id,
                    "total_documents": row.total_documents,
                    "total_words": row.total_words,
                    "avg_words_per_doc": row.avg_words_per_doc,
                    "total_tokens": row.total_tokens,
                    "unique_domains": row.unique_domains,
                    "successful_scrapes": row.successful_scrapes,
                    "failed_scrapes": row.failed_scrapes,
                    "success_rate": (row.successful_scrapes / row.total_documents * 100) if row.total_documents > 0 else 0,
                    "first_document_time": row.first_document_time.isoformat() if row.first_document_time else None,
                    "last_document_time": row.last_document_time.isoformat() if row.last_document_time else None,
                    "duration_minutes": round(duration_minutes, 2)
                }
                
            return {}
            
        except Exception as e:
            logger.error(f"Error retrieving job stats: {str(e)}")
            return {}

    async def check_authentication(self) -> bool:
        """
        Check if BigQuery credentials are valid and authenticated.
        
        Returns:
            bool: True if authentication is valid, False otherwise
        """
        try:
            if not self.enable_storage:
                return True  # Storage disabled, so no auth needed
                
            # Try a simple query to test authentication
            from google.cloud import bigquery
            
            if not self.client:
                logger.info("Creating BigQuery client for authentication check")
                self.client = bigquery.Client(project=self.project_id)
            
            # Simple test query that shouldn't cost much
            query = "SELECT 1 as test_value"
            
            # Run query with a short timeout (5 seconds)
            try:
                # Convert to async operation
                result = await asyncio.wait_for(
                    asyncio.to_thread(lambda: list(self.client.query(query).result(timeout=10))),
                    timeout=10.0
                )
                # Check if we got any results
                self._auth_checked = True
                if result and len(result) > 0:
                    logger.info("BigQuery authentication successful")
                    self._authenticated = True
                    self._auth_error = None
                    return True
                else:
                    logger.warning("BigQuery authentication check returned no results")
                    self._authenticated = False
                    self._auth_error = "No results from authentication test query"
                    self.enable_storage = False
                    return False
                    
            except asyncio.TimeoutError:
                logger.warning("BigQuery authentication check timed out")
                self._authenticated = False
                self._auth_checked = True
                self._auth_error = "Authentication check timed out"
                self.enable_storage = False
                return False
                
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"BigQuery authentication check failed: {error_msg}")
            self._authenticated = False
            self._auth_checked = True
            self._auth_error = error_msg
            self.enable_storage = False
            return False
    
    def get_auth_status(self) -> Tuple[bool, str]:
        """
        Get the current authentication status and error message.
        
        Returns:
            Tuple[bool, str]: (is_authenticated, error_message)
        """
        return self._authenticated, self._auth_error
    
    def disable_storage(self) -> None:
        """Disable BigQuery storage."""
        self.enable_storage = False
        logger.info("BigQuery storage disabled")
        
    def enable_storage(self) -> None:
        """Try to enable BigQuery storage by reinitializing."""
        try:
            self.enable_storage = True
            # Reinitialize
            self.client = None
            self._initialize_bigquery()
            logger.info("BigQuery storage enabled")
        except Exception as e:
            logger.error(f"Failed to enable BigQuery storage: {str(e)}")
            self.enable_storage = False
            self._auth_error = str(e)
            self._authenticated = False
