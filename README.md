# xpcs_gladier

XPCS Gladier for running the XPCS Reprocessing Flow.

## Installation

    python setup.py install
    
## Running 

Check the reprocessing.py file and add the following to `flow_input`:

    # Manifest destination, where files will be transferred for execution (theta)
    'manifest_destination': 'globus://08925f04-569f-11e7-bef8-22000b9a448b/projects/APSDataAnalysis/Automate_ryan/reprocessing/',
    # Destination MUST be within the directory defined in 'manifest_destination'
    'qmap_file': '/projects/APSDataAnalysis/Automate_ryan/reprocessing/sanat201903_qmap_S270_D54_lin.h5',
    
    # Your personal FuncX login/theta endpoints
    'funcx_endpoint_non_compute': '6c4323f4-a062-4551-a883-146a352a43f5',
    'funcx_endpoint_compute': '9f84f41e-dfb6-4633-97be-b46901e9384c',
    
After that, simply run the following:

    python xpcs_client/reprocessing.py
    
 