"""Utility functions for handling metadata in Jupyter notebooks."""

from typing import Dict, List, Set, Any, TypedDict, Tuple



##Result parser for XPCS
def parse_result_files(q_entry, local_path=None):
    entry_files = q_entry['entries'][0]['content']['files'] ## this is a list
    result_file = None
    log_file = None
    image_files = []
    globus_url = 'globus://74defd5b-5f61-42fc-bcc4-834c9f376a4f'
    for file in entry_files:
        if file['filename'].endswith('results.hdf'):
            result_file = file['url'] if local_path is None else file['url'].replace(globus_url, local_path)
        elif file['filename'].endswith('.log'):
            log_file = file['url'] if local_path is None else file['url'].replace(globus_url, local_path)
        elif file['filename'].endswith(('.png', '.jpg', '.jpeg')):
            image_files.append(file['url'] if local_path is None else file['url'].replace(globus_url, local_path))
    return result_file, log_file, image_files
    

class MetadataStructure(TypedDict):
    """Type definition for the metadata structure."""
    dc_keys: Set[str]
    project_metadata_keys: Set[str]
    file_keys: Set[str]

def recursively_collect_keys(data: Any, keys_set: Set[str]) -> None:
    """Recursively collect keys from nested dictionaries and lists.
    
    Args:
        data: The input data from which to extract keys.
        keys_set: The set in which keys will be accumulated.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            keys_set.add(key)
            recursively_collect_keys(value, keys_set)
    elif isinstance(data, list):
        for item in data:
            recursively_collect_keys(item, keys_set)

def recursively_collect_key_values(data: Any, prefix: str = "") -> List[Tuple[str, Any]]:
    """Recursively collect all key-value pairs from nested dictionaries and lists.
    
    Args:
        data: The input data from which to extract key-value pairs.
        prefix: The prefix to prepend to nested keys (used for recursion).
        
    Returns:
        A list of tuples containing (key, value) pairs, where keys are dot-notation
        paths to the values in the nested structure.
        
    Example:
        >>> data = {"a": {"b": 1, "c": [2, 3]}}
        >>> recursively_collect_key_values(data)
        [('a.b', 1), ('a.c.0', 2), ('a.c.1', 3)]
    """
    result: List[Tuple[str, Any]] = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            new_prefix = f"{prefix}.{key}" if prefix else key
            if isinstance(value, (dict, list)):
                result.extend(recursively_collect_key_values(value, new_prefix))
            else:
                result.append((new_prefix, value))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_prefix = f"{prefix}.{i}" if prefix else str(i)
            if isinstance(item, (dict, list)):
                result.extend(recursively_collect_key_values(item, new_prefix))
            else:
                result.append((new_prefix, item))
    else:
        result.append((prefix, data))
    
    return result

def extract_metadata_structure(response: Dict[str, Any]) -> MetadataStructure:
    """Extract and return the complete metadata structure from the response.
    
    This function analyzes the response from the Globus Search API and extracts
    all unique keys present in the dc (Dublin Core), project_metadata, and files
    sections of the metadata.
    
    Args:
        response: The response dictionary from Globus Search API containing 'gmeta' entries.
        
    Returns:
        A dictionary containing sets of unique keys found in each metadata section:
        - dc_keys: Set of keys from Dublin Core metadata
        - project_metadata_keys: Set of keys from project metadata
        - file_keys: Set of keys from file metadata
        
    Example:
        >>> response = {"gmeta": [...]}
        >>> structure = extract_metadata_structure(response)
        >>> print(structure["dc_keys"])
        {'title', 'creator', ...}
    """
    dc_keys: Set[str] = set()
    project_metadata_keys: Set[str] = set()
    file_keys: Set[str] = set()
    
    for entry in response["gmeta"]:
        # Extract Dublin Core keys
        if "entries" in entry and entry["entries"]:
            content = entry["entries"][0]["content"]
            
            # Extract DC keys recursively
            if "dc" in content:
                recursively_collect_keys(content["dc"], dc_keys)
            
            # Extract project metadata keys recursively
            if "project_metadata" in content:
                recursively_collect_keys(content["project_metadata"], project_metadata_keys)
            
            # Extract file keys if present
            if "files" in content:
                for file_entry in content["files"]:
                    file_keys.update(file_entry.keys())

    print("DC KEYS")
    print(dc_keys)
    print("PROJECT METADATA KEYS")
    print(project_metadata_keys)
    print("FILE KEYS")
    print(file_keys)

    return {
        "dc_keys": dc_keys,
        "project_metadata_keys": project_metadata_keys,
        "file_keys": file_keys
    }

def extract_entry_key_values(entry: Dict[str, Any]) -> Dict[str, List[Tuple[str, Any]]]:
    """Extract all key-value pairs from a single entry in the response.
    
    Args:
        entry: A single entry from the response's 'gmeta' list.
        
    Returns:
        A dictionary containing lists of (key, value) pairs for each section:
        - dc: List of (key, value) pairs from Dublin Core metadata
        - project_metadata: List of (key, value) pairs from project metadata
        - files: List of (key, value) pairs from file metadata
        
    Example:
        >>> entry = response["gmeta"][0]
        >>> values = extract_entry_key_values(entry)
        >>> for key, value in values["dc"]:
        ...     print(f"{key}: {value}")
    """
    result: Dict[str, List[Tuple[str, Any]]] = {
        "dc": [],
        "project_metadata": [],
        "files": []
    }
    
    if "entries" in entry and entry["entries"]:
        content = entry["entries"][0]["content"]
        
        if "dc" in content:
            result["dc"] = recursively_collect_key_values(content["dc"])
        
        if "project_metadata" in content:
            result["project_metadata"] = recursively_collect_key_values(content["project_metadata"])
        
        if "files" in content:
            for file_entry in content["files"]:
                result["files"].extend(recursively_collect_key_values(file_entry))
    
    return result 