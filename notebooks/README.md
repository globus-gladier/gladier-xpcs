# XPCS Notebooks

This directory contains Jupyter notebooks and utility modules for working with X-ray Photon Correlation Spectroscopy (XPCS) data using the Globus Search API.

## Directory Structure

```
notebooks/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── search_remote.ipynb       # Main notebook for remote XPCS data search
├── search_milli.ipynb        # Notebook for millimeter-scale XPCS data
├── metadata.py               # Metadata processing utilities
├── nb_plot.py                # Plotting utilities for HDF5 data
├── remote.py                 # Remote functions for wget with globus-https
```

## Setup

1. Install the required dependencies:
```bash
cd notebooks
pip install -r requirements.txt
```

2. Start Jupyter:
```bash
jupyter notebook
```

## Core Modules

### `metadata.py`
Contains utility functions for processing metadata from Globus Search API responses:

- `extract_metadata_structure(response, verbose=False)`: Extracts all available search keys from the response
- `extract_entry_key_values(entry, verbose=False)`: Extracts key-value pairs from a single entry
- `parse_result_files(q_entry, local_path=None)`: Parses file information from search results

### `remote.py`
...
...

### `nb_plot.py`
Provides plotting utilities for HDF5 data visualization:

- `start_new_plot(title, logx=False, logy=False)`: Initialize a new plot
- `plot_hdf_overlay(file, x, y, label)`: Add data from HDF5 file to current plot
- `show_plot()`: Display the final plot

## File Access Modes: Local vs Remote

The notebooks support two modes of file access depending on whether the data files are accessible locally or need to be accessed remotely.

### Remote Access (Default)
When files are accessed via Globus URLs, no local path is specified:

```python
from metadata import parse_result_files

# Remote access - files accessed via Globus URLs
result_file, log_file, image_files = parse_result_files(q_entry)
print(f"Result file: {result_file}")
# Output: globus://74defd5b-5f61-42fc-bcc4-834c9f376a4f/path/to/file.hdf
```

**Use remote access when:**
- Running notebooks on a machine without direct access to the data storage
- Working with data distributed across multiple Globus endpoints
- Need to access data through Globus Transfer protocols

### Local Access
When files are accessible on the local filesystem, specify a `local_path`:

```python
from metadata import parse_result_files

# Local access - replace Globus URLs with local paths
local_base_path = "/data/xpcs"
result_file, log_file, image_files = parse_result_files(q_entry, local_path=local_base_path)
print(f"Result file: {result_file}")
# Output: /data/xpcs/path/to/file.hdf
```

**Use local access when:**
- Running notebooks on a machine with direct filesystem access to the data
- Working with mounted network filesystems
- Data has been previously downloaded/synchronized locally

### Example Usage Patterns

#### Pattern 1: Search and Analyze Remotely
```python
import globus_sdk
from metadata import extract_metadata_structure, parse_result_files
from nb_plot import start_new_plot, plot_hdf_overlay, show_plot

# Search for data
# ... search code ...

# Parse files for remote access
for entry in response["gmeta"]:
    result_file, log_file, image_files = parse_result_files({"entries": [entry]})
    # Use Globus SDK to access files
```

#### Pattern 2: Search and Analyze Locally
```python
import globus_sdk
from metadata import extract_metadata_structure, parse_result_files
from nb_plot import start_new_plot, plot_hdf_overlay, show_plot

# Search for data
# ... search code ...

# Parse files for local access
local_data_path = "/mnt/xpcs_data"
for entry in response["gmeta"]:
    result_file, log_file, image_files = parse_result_files(
        {"entries": [entry]}, 
        local_path=local_data_path
    )
    
    # Direct file access for plotting
    start_new_plot("XPCS Analysis", logx=True, logy=True)
    plot_hdf_overlay(result_file, label="Sample Data")
    show_plot()
```


## Metadata Structure

The metadata extraction functions return keys in dot notation format:
- `dc.creators.name` - Dublin Core creator information
- `dc.dates.date` - Dublin Core date information
- `project_metadata.experiment.type` - Project-specific metadata
- `files.filename` - File metadata

Use `verbose=True` with metadata functions to see detailed structure information:

```python
from metadata import extract_metadata_structure

# Get all available search keys
keys = extract_metadata_structure(response, verbose=True)
print(f"Found {len(keys)} unique search keys")
```