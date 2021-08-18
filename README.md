# xpcs_gladier

XPCS Gladier for running the XPCS Reprocessing Flow.

## Installation

We highly encourage using [miniconda](miniconda)

### Main Package
    
    conda create -n gladier python=3.8
    conda activate gladier

    git clone https://github.com/globus-gladier/gladier-xpcs.git
    cd gladier-xpcs
    python setup.py develop

    ##install DM scripts
    cp  scripts/dm/* /home/beams10/8IDIUSER/DM_Workflows/xpcs8/
    
    cd /home/beams10/8IDIUSER/DM_Workflows/xpcs8/
    dm-add-workflow --py-spec workflow-xpcs8-01-gladier.py

## Reprocessing

XPCS Reprocessing takes data already published in the portal and re-runs it on corr with
a customized (with a qmap file) hdf file. Reprocessing also has an extra step to rename
the dataset to publish it under a different title in the portal. 

Although scripts exist here to test the reprocessing flow, the actual production flow is
deployed separately on the portal. The portal installs the `gladier_xpcs` package and
imports the Gladier Client.

The main reprocessing client is at `gladier_xpcs/client_reprocess.py`. A script for 
testing reprocessing is located at `scripts/xpcs_reproc_client.py`. Reprocessing
shares some tools with the online processing flow, but contains a handful of custom
tools under `gladier_xpcs/reprocessing_tools`.

### Running The Reprocessing Flow

You need to setup your deployment on Theta before you can run reprocessing. This includes
setting up

* a 'login' and 'compute' funcx-endpoint on theta
* a 'processing' directory on theta you have read/write access to

Make sure you are also in the XPCS Developers Globus group to access XPCS datasets which
have already been published.

To test a reprocessing flow, ensure Test run a reprocessing flow with the following:

```
cd scripts/
python xpcs_reproc_client.py
```
