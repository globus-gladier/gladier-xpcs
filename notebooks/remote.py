import requests



def get_log_in_memory(log_file_url, token):
    response = requests.get(log_file_url, headers={'Authorization': f'Bearer {token}'})
    response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
    file_content_in_memory: bytes = response.content
    log_file_content = file_content_in_memory.decode('utf-8')
    # Decode bytes to string and print as text file
    return log_file_content

def get_result_file(result_file_url, token):
    import subprocess
    import os
    
    # Extract filename from URL
    filename = os.path.basename(result_file_url)
    
    # Use wget to download the file with authorization header
    wget_command = [
        'wget',
        '-q',
        '--header', f'Authorization: Bearer {token}',
        '-O', filename,  # Save to current directory with original filename
        result_file_url
    ]
    
    print('Downloading file: ' + result_file_url)
    # Execute wget command
    subprocess.run(wget_command, check=True)
    
    # Return the local filename for reference
    return filename
