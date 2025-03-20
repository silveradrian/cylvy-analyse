#!/usr/bin/env python3
"""
Prompt Loader Module

This module provides functionality for loading YAML prompt configurations,
validating their structure, and making them available for the application.
"""

import os
import yaml
import logging
from typing import List, Dict, Any, Optional
import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("prompt_loader")

# Default directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROMPTS_DIR = os.path.join(BASE_DIR, "prompts", "yaml")
DEFAULT_PROMPTS = ["content_analysis.yaml", "strategic_imperatives.yaml"]

def ensure_directories():
    """Ensure that necessary directories exist."""
    os.makedirs(PROMPTS_DIR, exist_ok=True)
    logger.info(f"Ensured prompt directory exists: {PROMPTS_DIR}")

def create_default_prompts():
    """Create default prompt files if they don't exist."""
    if not os.path.exists(PROMPTS_DIR) or not os.listdir(PROMPTS_DIR):
        logger.info("Creating default prompt files...")
        ensure_directories()
        
        # Content analysis prompt template
        content_analysis = {
            "name": "content_analysis",
            "category_code": "ca",
            "description": "Analyzes content for relevance and audience targeting",
            "model": "gpt-4",
            "system_instructions": "You are an expert content analyst. Your task is to analyze the given text and extract key insights about its target audience, content quality, and relevance to business objectives.",
            "analysis_prompt": """Analyze the following content and provide structured insights about:
1. Target audience
2. Content quality and clarity
3. Key topics and themes
4. Business relevance

Format your response using the field names and delimiter provided below:""",
            "output_fields": [
                {
                    "name": "ca_target_audience",
                    "field_type": "text",
                    "details": "The primary target audience for this content"
                },
                {
                    "name": "ca_quality_score",
                    "field_type": "int",
                    "details": "Content quality score (1-10)"
                },
                {
                    "name": "ca_key_themes",
                    "field_type": "text",
                    "details": "Main themes or topics covered"
                },
                {
                    "name": "ca_business_relevance",
                    "field_type": "int",
                    "details": "Relevance to business objectives (1-10)"
                },
                {
                    "name": "ca_improvement_suggestions",
                    "field_type": "text",
                    "details": "Suggestions for improving the content"
                }
            ],
            "delimiter": "|||"
        }
        
        # Strategic imperatives prompt template
        strategic_imperatives = {
            "name": "strategic_imperatives",
            "category_code": "si",
            "description": "Identifies strategic focus areas within content",
            "model": "gpt-4",
            "system_instructions": "You are an analyst specialized in identifying strategic imperatives in business content. Your task is to analyze the given text and identify the primary strategic imperative it focuses on.",
            "analysis_prompt": """Based on the provided content, identify the primary strategic imperative that the document focuses on.
Choose from: Mission-Critical Software, Financial Services, Security, Reliability, Customer Centricity, 
Innovation, Modernization, or Transformation.

Then, provide a brief rationale for your choice and assign scores (0-10) for each strategic imperative 
based on how prominently they feature in the content.

Format your response exactly as follows:

si_primary_si ||| [Primary Strategic Imperative]
si_primary_si_rationale ||| [Brief explanation of why this is the primary focus]
si_other_theme ||| [Any other notable theme or None]
si_score_mission_critical_software ||| [Score 0-10]
si_score_financial_services ||| [Score 0-10]
si_score_security ||| [Score 0-10]
si_score_reliability ||| [Score 0-10]
si_score_customer_centricity ||| [Score 0-10]
si_score_innovation ||| [Score 0-10]
si_score_modernization ||| [Score 0-10]
si_score_transformation ||| [Score 0-10]""",
            "output_fields": [
                {
                    "name": "si_primary_si",
                    "field_type": "text",
                    "details": "The primary strategic imperative identified in the content"
                },
                {
                    "name": "si_primary_si_rationale",
                    "field_type": "text",
                    "details": "Rationale for the primary strategic imperative choice"
                },
                {
                    "name": "si_other_theme",
                    "field_type": "text",
                    "details": "Any other notable theme identified in the content"
                },
                {
                    "name": "si_score_mission_critical_software",
                    "field_type": "int",
                    "details": "Score for Mission-Critical Software (0-10)"
                },
                {
                    "name": "si_score_financial_services",
                    "field_type": "int",
                    "details": "Score for Financial Services (0-10)"
                },
                {
                    "name": "si_score_security",
                    "field_type": "int",
                    "details": "Score for Security (0-10)"
                },
                {
                    "name": "si_score_reliability",
                    "field_type": "int",
                    "details": "Score for Reliability (0-10)"
                },
                {
                    "name": "si_score_customer_centricity",
                    "field_type": "int",
                    "details": "Score for Customer Centricity (0-10)"
                },
                {
                    "name": "si_score_innovation",
                    "field_type": "int",
                    "details": "Score for Innovation (0-10)"
                },
                {
                    "name": "si_score_modernization",
                    "field_type": "int",
                    "details": "Score for Modernization (0-10)"
                },
                {
                    "name": "si_score_transformation",
                    "field_type": "int",
                    "details": "Score for Transformation (0-10)"
                }
            ],
            "delimiter": "|||"
        }
        
        # Save the default prompts
        with open(os.path.join(PROMPTS_DIR, "content_analysis.yaml"), "w") as f:
            yaml.dump(content_analysis, f, default_flow_style=False)
            
        with open(os.path.join(PROMPTS_DIR, "strategic_imperatives.yaml"), "w") as f:
            yaml.dump(strategic_imperatives, f, default_flow_style=False)
            
        logger.info("Created default prompt files successfully")

def validate_prompt_config(config: Dict[str, Any]) -> bool:
    """
    Validate a prompt configuration.
    
    Args:
        config: The prompt configuration to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_fields = [
        "name", "analysis_prompt", "output_fields", "system_instructions"
    ]
    
    # Check if all required fields are present
    for field in required_fields:
        if field not in config:
            logger.error(f"Missing required field in prompt config: {field}")
            return False
    
    # Check if output_fields is a list with at least one item
    if not isinstance(config["output_fields"], list) or len(config["output_fields"]) == 0:
        logger.error("output_fields must be a non-empty list")
        return False
    
    # Check if each output_field has the required structure
    for field in config["output_fields"]:
        if not isinstance(field, dict):
            logger.error("Each output_field must be a dictionary")
            return False
        
        if not all(key in field for key in ["name", "field_type", "details"]):
            logger.error("Each output_field must have 'name', 'field_type', and 'details'")
            return False
    
    return True

def load_prompt_configs(prompt_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load all prompt configurations from YAML files.
    
    Args:
        prompt_dir: Directory containing prompt YAML files (optional)
        
    Returns:
        List of prompt configurations
    """
    if prompt_dir is None:
        prompt_dir = PROMPTS_DIR
    
    ensure_directories()
    create_default_prompts()
    
    configs = []
    
    try:
        # Get all YAML files in the directory
        yaml_files = [f for f in os.listdir(prompt_dir) 
                     if f.endswith(('.yaml', '.yml'))]
        
        if not yaml_files:
            logger.warning(f"No YAML files found in {prompt_dir}")
            return configs
        
        for yaml_file in yaml_files:
            try:
                with open(os.path.join(prompt_dir, yaml_file), 'r') as f:
                    config = yaml.safe_load(f)
                
                # Skip empty configs
                if not config:
                    logger.warning(f"Empty or invalid YAML file: {yaml_file}")
                    continue
                
                # Validate the config
                if not validate_prompt_config(config):
                    logger.warning(f"Invalid prompt configuration in {yaml_file}")
                    continue
                
                # Add loading timestamp and filename
                config["loaded_at"] = datetime.datetime.now().isoformat()
                config["source_file"] = yaml_file
                
                configs.append(config)
                logger.info(f"Loaded prompt configuration: {config.get('name', 'unnamed')} from {yaml_file}")
                
            except Exception as e:
                logger.error(f"Error loading {yaml_file}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error scanning prompt directory: {str(e)}")
    
    logger.info(f"Loaded {len(configs)} prompt configurations")
    return configs

def get_prompt_by_name(name: str) -> Optional[Dict[str, Any]]:
    """
    Get a specific prompt configuration by name.
    
    Args:
        name: Name of the prompt configuration to retrieve
        
    Returns:
        The prompt configuration or None if not found
    """
    configs = load_prompt_configs()
    
    for config in configs:
        if config.get("name") == name:
            return config
    
    return None

def list_available_prompts() -> List[Dict[str, str]]:
    """
    List all available prompts with basic information.
    
    Returns:
        List of dictionaries containing prompt name, description, and category
    """
    configs = load_prompt_configs()
    
    return [
        {
            "name": config.get("name", "unnamed"),
            "description": config.get("description", "No description available"),
            "category_code": config.get("category_code", ""),
            "model": config.get("model", "gpt-4")
        }
        for config in configs
    ]

class PromptLoader:
    """
    Class to load and manage prompt configurations from YAML files.
    """
    
    def __init__(self, prompts_dir: str = None):
        """
        Initialize the prompt loader.
        
        Args:
            prompts_dir: Directory containing prompt YAML files (optional)
        """
        self.prompts_dir = prompts_dir if prompts_dir else PROMPTS_DIR
        self.ensure_prompt_dir()
        
    def ensure_prompt_dir(self):
        """Ensure the prompts directory exists."""
        ensure_directories()
        
    def load_prompts(self) -> Dict[str, Any]:
        """
        Load all prompt configurations and return as a dictionary.
        
        Returns:
            Dictionary mapping prompt names to their configurations
        """
        prompt_list = load_prompt_configs(self.prompts_dir)
        
        # Convert list to dictionary with prompt names as keys
        prompts_dict = {}
        for prompt in prompt_list:
            name = prompt.get('name')
            if name:
                # Create compatibility with analyzer.py expectations
                prompt_config = {
                    'name': prompt.get('name', ''),
                    'description': prompt.get('description', ''),
                    'model': prompt.get('model', 'gpt-4'),
                    'system_message': prompt.get('system_instructions', ''),
                    'user_message': prompt.get('analysis_prompt', ''),
                    'temperature': prompt.get('temperature', 0.3),
                    'max_tokens': prompt.get('max_tokens', 1500),
                    'output_fields': prompt.get('output_fields', []),
                    'delimiter': prompt.get('delimiter', '|||'),
                }
                prompts_dict[name] = prompt_config
                
        return prompts_dict
    
    def get_prompt_configs(self) -> List[Dict[str, Any]]:
        """
        Get all prompt configurations as a list for UI display.
        
        Returns:
            List of dictionaries with prompt info
        """
        available_prompts = list_available_prompts()
        
        # Format for analyzer.py compatibility
        return [
            {
                'id': prompt.get('name', ''),
                'name': prompt.get('name', ''),
                'description': prompt.get('description', ''),
                'model': prompt.get('model', 'gpt-4')
            }
            for prompt in available_prompts
        ]
    
    def get_prompt_by_id(self, prompt_id: str) -> Dict[str, Any]:
        """
        Get a prompt configuration by its ID.
        
        Args:
            prompt_id: The prompt ID (same as name)
            
        Returns:
            Prompt configuration or empty dict if not found
        """
        prompt = get_prompt_by_name(prompt_id)
        
        if not prompt:
            return {}
            
        # Format for analyzer.py compatibility
        return {
            'name': prompt.get('name', ''),
            'description': prompt.get('description', ''),
            'model': prompt.get('model', 'gpt-4'),
            'system_message': prompt.get('system_instructions', ''),
            'user_message': prompt.get('analysis_prompt', ''),
            'temperature': prompt.get('temperature', 0.3),
            'max_tokens': prompt.get('max_tokens', 1500),
            'output_fields': prompt.get('output_fields', []),
            'delimiter': prompt.get('delimiter', '|||'),
        }

if __name__ == "__main__":
    # This allows for testing the module directly
    print("Available prompts:")
    prompts = list_available_prompts()
    for prompt in prompts:
        print(f"- {prompt['name']}: {prompt['description']}")
    
    # Test the PromptLoader class
    print("\nTesting PromptLoader class:")
    loader = PromptLoader()
    prompt_dict = loader.load_prompts()
    print(f"Loaded {len(prompt_dict)} prompts via PromptLoader class")
