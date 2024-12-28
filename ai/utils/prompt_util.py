import os
from string import Template

def get_prompt_by_name(prompt_name: str, variables: dict = None) -> str:
    """
    Load a prompt from the prompts directory by name and format it with variables
    
    Args:
        prompt_name (str): Name of the prompt file without extension
        variables (dict): Variables to substitute in the prompt
        
    Returns:
        str: Formatted prompt content
    """
    prompt_path = os.path.join("prompts", f"{prompt_name}.md")
    
    try:
        with open(prompt_path, "r") as f:
            template = Template(f.read())
            
        if variables:
            return template.safe_substitute(variables)
        return template.template
            
    except FileNotFoundError:
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
