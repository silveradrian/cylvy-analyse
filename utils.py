def extract_structured_fields(analysis_text, prefix=''):
    """
    Extract structured fields from analysis text that contains field|value format
    
    Args:
        analysis_text (str): Text containing fields in format: field ||| value
        prefix (str): Optional prefix to add to field names (e.g., 'ca_' for content analysis)
        
    Returns:
        dict: Dictionary of extracted fields
    """
    if not analysis_text:
        return {}
    
    structured_fields = {}
    
    # Split by lines and process each line
    for line in analysis_text.split('\n'):
        line = line.strip()
        if not line or '|||' not in line:
            continue
            
        # Split the line by the triple pipe separator
        parts = line.split('|||', 1)
        if len(parts) != 2:
            continue
            
        field_name = parts[0].strip()
        field_value = parts[1].strip()
        
        # Skip header/section lines
        if field_name.startswith('--') or field_name.startswith('PART'):
            continue
            
        # Convert field name to snake_case for consistent database storage
        field_name = field_name.lower().replace(' ', '_').replace('-', '_')
        
        # Add prefix if provided
        if prefix:
            field_name = f"{prefix}_{field_name}"
        
        structured_fields[field_name] = field_value
        
    return structured_fields
