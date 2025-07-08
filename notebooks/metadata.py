"""Utility functions for handling metadata in Jupyter notebooks."""

from pprint import pprint
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

def recursively_collect_keys_with_prefix(data: Any, keys_set: Set[str], prefix: str = "") -> None:
    """Recursively collect keys with dot notation, ignoring list indices.
    
    Args:
        data: The input data from which to extract keys.
        keys_set: The set in which keys will be accumulated.
        prefix: The prefix to prepend to nested keys (used for recursion).
    """
    if isinstance(data, dict):
        for key, value in data.items():
            new_prefix = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                recursively_collect_keys_with_prefix(value, keys_set, new_prefix)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # For lists of dictionaries, process the first item to get structure
                recursively_collect_keys_with_prefix(value[0], keys_set, new_prefix)
            else:
                keys_set.add(new_prefix)
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        # For lists of dictionaries at root level, process the first item
        recursively_collect_keys_with_prefix(data[0], keys_set, prefix)

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

def extract_metadata_structure(response: Dict[str, Any], verbose: bool = False) -> List[str]:
    """Extract and return the complete metadata structure from the response.
    
    This function analyzes the response from the Globus Search API and extracts
    all unique keys present in the dc (Dublin Core), project_metadata, and files
    sections of the metadata.
    
    Args:
        response: The response dictionary from Globus Search API containing 'gmeta' entries.
        verbose: If True, prints detailed information about the extracted keys using pprint.
        
    Returns:
        A list of valid search keys combining all metadata sections:
        - DC keys in dot notation (e.g., 'dc.creators.name', 'dc.dates.date')
        - Project metadata keys
        - File keys
        
    Example:
        >>> response = {"gmeta": [...]}
        >>> valid_keys = extract_metadata_structure(response, verbose=True)
        >>> print(valid_keys)
        ['dc.creators.name', 'dc.dates.date', 'project_metadata.experiment', ...]
    """
    dc_keys: Set[str] = set()
    project_metadata_keys: Set[str] = set()
    file_keys: Set[str] = set()
    
    for entry in response["gmeta"]:
        # Extract Dublin Core keys
        if "entries" in entry and entry["entries"]:
            content = entry["entries"][0]["content"]
            
            # Extract DC keys using dot notation, ignoring list indices
            if "dc" in content:
                recursively_collect_keys_with_prefix(content["dc"], dc_keys, "dc")
            
            # Extract project metadata keys using dot notation, ignoring list indices
            if "project_metadata" in content:
                recursively_collect_keys_with_prefix(content["project_metadata"], project_metadata_keys, "project_metadata")
            
            # Extract file keys using dot notation, ignoring list indices
            if "files" in content:
                for file_entry in content["files"]:
                    recursively_collect_keys_with_prefix(file_entry, file_keys, "files")

    # Combine all keys into a single list
    valid_search_keys = list(dc_keys) + list(project_metadata_keys) + list(file_keys)
    
    if verbose:
        print("\n=== DC KEYS ===")
        pprint(sorted(dc_keys))
        print("\n=== PROJECT METADATA KEYS ===")
        pprint(sorted(project_metadata_keys))
        print("\n=== FILE KEYS ===")
        pprint(sorted(file_keys))
        print("\n=== ALL VALID SEARCH KEYS ===")
        pprint(sorted(valid_search_keys))

    return sorted(valid_search_keys)

def extract_entry_key_values(entry: Dict[str, Any], verbose: bool = False) -> List[Tuple[str, Any]]:
    """Extract all key-value pairs from a single entry in the response.
    
    Args:
        entry: A single entry from the response's 'gmeta' list.
        verbose: If True, prints detailed information about the extracted key-value pairs using pprint.
        
    Returns:
        A single list of (key, value) pairs combining all metadata sections with dot notation:
        - DC metadata with 'dc.' prefix
        - Project metadata with 'project_metadata.' prefix  
        - File metadata with 'files.' prefix
        
    Example:
        >>> entry = response["gmeta"][0]
        >>> values = extract_entry_key_values(entry, verbose=True)
        >>> for key, value in values:
        ...     print(f"{key}: {value}")
    """
    result: List[Tuple[str, Any]] = []
    
    if "entries" in entry and entry["entries"]:
        content = entry["entries"][0]["content"]
        
        if "dc" in content:
            result.extend(recursively_collect_key_values(content["dc"], "dc"))
        
        if "project_metadata" in content:
            result.extend(recursively_collect_key_values(content["project_metadata"], "project_metadata"))
        
        if "files" in content:
            for file_entry in content["files"]:
                result.extend(recursively_collect_key_values(file_entry, "files"))
    
    if verbose:
        print("\n=== ENTRY KEY-VALUE PAIRS ===")
        if result:
            pprint(result[:10])  # Show first 10 pairs to avoid overwhelming output
            if len(result) > 10:
                print(f"... and {len(result) - 10} more pairs")
        else:
            print("No key-value pairs found")
    
    return result 