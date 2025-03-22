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
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("prompt_loader")

# Default directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROMPTS_DIR = os.path.join(BASE_DIR, "prompts", "yaml")

def ensure_directories():
    """Ensure that necessary directories exist."""
    os.makedirs(PROMPTS_DIR, exist_ok=True)
    logger.info(f"Ensured prompt directory exists: {PROMPTS_DIR}")

def validate_prompt_config(config: Dict[str, Any]) -> bool:
    """
    Validate a prompt configuration using a very flexible approach.
    
    Args:
        config: The prompt configuration to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    # Only ensure it has a name - we'll be lenient with everything else
    if not config.get('name'):
        logger.error("Prompt configuration missing required 'name' field")
        return False
    
    # Consider it valid for maximum flexibility
    return True

def load_yaml_file(file_path: str) -> Dict[str, Any]:
    """
    Load a YAML file with explicit error handling.
    
    Args:
        file_path: Path to the YAML file
        
    Returns:
        Dictionary with YAML content or empty dict on error
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = yaml.safe_load(f)
            logger.info(f"Successfully loaded YAML file: {file_path}")
            return content
    except Exception as e:
        logger.error(f"Error loading YAML file {file_path}: {str(e)}")
        return {}

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
    
    # Only ensure directory exists, don't create default prompts
    ensure_directories()
    
    configs = []
    
    try:
        # Get all YAML files in the directory and subdirectories
        yaml_pattern = os.path.join(prompt_dir, "**/*.yaml")
        yml_pattern = os.path.join(prompt_dir, "**/*.yml")
        yaml_files = glob.glob(yaml_pattern, recursive=True) + glob.glob(yml_pattern, recursive=True)
        
        if not yaml_files:
            logger.warning(f"No YAML files found in {prompt_dir}")
            return configs
        
        logger.info(f"Found {len(yaml_files)} YAML files: {[os.path.basename(f) for f in yaml_files]}")
        
        for yaml_file in yaml_files:
            try:
                # Load the YAML file
                config = load_yaml_file(yaml_file)
                
                # Skip empty configs
                if not config:
                    logger.warning(f"Empty or invalid YAML file: {yaml_file}")
                    continue
                
                # Use filename as name if not specified in file
                if 'name' not in config:
                    base_name = os.path.basename(yaml_file)
                    config['name'] = os.path.splitext(base_name)[0]
                    logger.info(f"Using filename as name for {yaml_file}: {config['name']}")
                
                # Add loading timestamp and filename
                config["loaded_at"] = datetime.datetime.now().isoformat()
                config["source_file"] = os.path.basename(yaml_file)
                
                configs.append(config)
                logger.info(f"Successfully added prompt configuration: {config.get('name')} from {os.path.basename(yaml_file)}")
                
            except Exception as e:
                logger.error(f"Error processing {yaml_file}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error scanning prompt directory: {str(e)}")
    
    logger.info(f"Loaded {len(configs)} prompt configurations: {[c.get('name') for c in configs]}")
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
    
    # Try also with filename-based matching (without extension)
    for config in configs:
        source_file = config.get("source_file", "")
        if source_file:
            file_name = os.path.splitext(source_file)[0]
            if file_name == name:
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
        
        # Debug output to check what's being loaded
        logger.info(f"Raw prompt list has {len(prompt_list)} items")
        if prompt_list:
            for p in prompt_list:
                logger.info(f"Found prompt: {p.get('name')} from file {p.get('source_file')}")
        
        # Convert list to dictionary with prompt names as keys
        prompts_dict = {}
        for prompt in prompt_list:
            name = prompt.get('name')
            if name:
                # Create compatibility with analyzer.py expectations
                prompt_config = {
                    'name': name,
                    'description': prompt.get('description', ''),
                    'model': prompt.get('model', 'gpt-4'),
                    'system_message': prompt.get('system_instructions', prompt.get('system_message', '')),
                    'user_message': prompt.get('analysis_prompt', prompt.get('user_message', '')),
                    'temperature': prompt.get('temperature', 0.3),
                    'max_tokens': prompt.get('max_tokens', 1500),
                    'output_fields': prompt.get('output_fields', []),
                    'delimiter': prompt.get('delimiter', '|||'),
                }
                prompts_dict[name] = prompt_config
                logger.info(f"Added prompt '{name}' to dictionary")
                
                # Also index by filename without extension if available
                source_file = prompt.get('source_file', '')
                if source_file:
                    file_name = os.path.splitext(source_file)[0]
                    if file_name != name:
                        prompts_dict[file_name] = prompt_config
                        logger.info(f"Also added under filename key '{file_name}'")
                        
        return prompts_dict
    
    def get_prompt_configs(self) -> List[Dict[str, Any]]:
        """
        Get all prompt configurations as a list for UI display.
        
        Returns:
            List of dictionaries with prompt info
        """
        available_prompts = list_available_prompts()
        logger.info(f"Prompts for UI: {[p.get('name') for p in available_prompts]}")
        
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
            'system_message': prompt.get('system_instructions', prompt.get('system_message', '')),
            'user_message': prompt.get('analysis_prompt', prompt.get('user_message', '')),
            'temperature': prompt.get('temperature', 0.3),
            'max_tokens': prompt.get('max_tokens', 1500),
            'output_fields': prompt.get('output_fields', []),
            'delimiter': prompt.get('delimiter', '|||'),
        }
    
    def get_prompt_by_name(self, name: str) -> Dict[str, Any]:
        """Alias for get_prompt_by_id"""
        return self.get_prompt_by_id(name)

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
    for name, config in prompt_dict.items():
        print(f"  - {name}")
