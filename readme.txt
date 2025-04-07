Cylvy Analyser
Content Analysis Tool for Websites and Documents
Python 3.8+ License: MIT

Cylvy Analyser is a powerful web application that enables automated content analysis of websites and documents using advanced AI models. The tool extracts insights from URLs, PDFs, Word documents, and PowerPoint presentations, organizing the extracted information into structured data.

Cylvy Analyser Dashboard

Features
Multi-format Content Analysis: Analyze HTML web pages, PDFs, DOCX, and PPTX files
Batch Processing: Upload CSV files containing multiple URLs with custom metadata
Intelligent Scraping: Enhanced web scraping with automatic retry mechanisms
Structured Data Extraction: Extract field-value pairs from content using AI models
Job Management: Track analysis progress with a comprehensive job dashboard
Data Export: Export results to CSV or Excel for further analysis
Customizable Prompts: Configure analysis templates for different use cases
Local File Support: Analyze files from your local filesystem
Installation
Prerequisites
Python 3.8 or higher
SQLite (included in Python)
Required Python packages (see requirements.txt)
Setup
Clone the repository:

bash
git clone https://github.com/silveradrian/cylvy-analyse.git
cd cylvy-analyse
Create a virtual environment:

bash
python -m venv venv
source venv/bin/activate  # On Windows, use venv\Scripts\activate
Install dependencies:

bash
pip install -r requirements.txt
Create a .env file with your API keys:

Code
OPENAI_API_KEY=your_openai_api_key
SCRAPINGBEE_API_KEY=your_scrapingbee_api_key
Run the application:

bash
python app.py
Open your browser and navigate to http://localhost:5000

Configuration
Environment Variables
Variable	Description	Default
OPENAI_API_KEY	OpenAI API key for content analysis	-
SCRAPINGBEE_API_KEY	ScrapingBee API key for advanced web scraping	-
PORT	Port to run the Flask server	5000
FLASK_ENV	Flask environment (development/production)	production
MAX_CONCURRENCY	Maximum concurrent requests	3
OPENAI_RATE_LIMIT	Rate limit for OpenAI API calls per minute	60
SCRAPINGBEE_RATE_LIMIT	Rate limit for ScrapingBee API calls per minute	50
Usage
Web Interface
Home Page: Start a new analysis job or view past jobs
URL Input: Enter URLs manually or upload a CSV file
Prompt Selection: Choose analysis templates for different insights
Job Monitoring: Track progress and view results in real-time
Results View: Explore structured data, analysis text, and metrics
CSV Upload Format
Upload a CSV file with the following columns:

url (Required): URL to analyze
company_name: Name of the company associated with the URL
company_description: Description of the company
company_industry: Industry classification
company_revenue: Company revenue information
content_type: Specify content type (html, pdf, docx, pptx, auto)
force_browser: Boolean to force browser rendering (true/yes/1)
Example:

Code
url,company_name,company_description,company_industry,content_type
https://example.com,Acme Corp,Widget manufacturer,"Manufacturing, Technology",html
https://example.org/doc.pdf,XYZ Inc,Software provider,Technology,pdf
Local File Analysis
To analyze local files, use the file:// protocol in your URL:

Code
file://C:\path\to\document.pdf
Prompt Templates
The system uses configurable prompt templates defined in JSON format. Templates include:

System message (context for the AI)
User message template (instructions and placeholders)
Model selection
Temperature and token settings
Place custom prompt templates in the prompts directory to extend analysis capabilities.

Architecture
Cylvy Analyser is built with a modular architecture:

Flask Web Application: Handles HTTP requests and renders UI
Content Analyzer: Core engine for processing content
Database Manager: SQLite-based persistence layer
Prompt Loader: Loads and manages prompt configurations
URL Processor: Handles batch processing with controlled concurrency
Development
Project Structure
Code
cylvy-analyse/
├── app.py               # Main Flask application
├── analyzer.py          # Content analysis engine
├── db_manager.py        # Database interface
├── prompt_loader.py     # Prompt configuration manager
├── utils.py             # Utility functions
├── templates/           # HTML templates
├── static/              # Static assets (CSS, JS)
├── prompts/             # Prompt configuration files
└── data/                # Data storage directory
Adding New Features
Fork the repository
Create a feature branch
Add tests for your feature
Ensure all tests pass
Submit a pull request
Troubleshooting
Common Issues
Scraping Errors: Check if the target website blocks automated access
OpenAI API Errors: Verify API key and rate limits
PDF Extraction Issues: Install optional dependencies for better extraction
License
This project is licensed under the MIT License - see the LICENSE file for details.

Contact
Project maintained by Adrian Silver

