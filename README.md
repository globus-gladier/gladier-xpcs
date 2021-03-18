# XPCS

Suresh Narayanan's time series spectroscopy work uses Theta to analyze APS's data (beamline 8-ID) on-demand.
The flow uses XPCS-Eigen's Corr tool to process two files: an input imm file and an output hdf5 file.
We use Automate to move data to ALCF as it is generated. This is achieved by modifying their
their DM flow to call a script (triggers/automate_xpcs.py) which generates an input doc for Automate and kicks off
the flow. Once data are processed at ALCF we also use funcX to extract metadata (tools/xpcs_metadata.py) and
generate plots (tools/xpcs_plots.py). The final step of the flow is to use Pilot to ingest all of this data
into a search index, populating the XPCS data porals.

This flow uses the backfill queue we have on Theta.

Repo Contents
-------------

This repository contains the following files.

Flows
~~~~~
- xpcs.py: A notebook to create each of the funcX functions and the automate flow.

Tools
~~~~~
- xpcs_metadata.py: A script to extract metadata from the hdf5 file.
- xpcs_plots.py: A script to generate the plots for the portal.
- xpcs_qc.py: A quality control script to ensure the fields necessary for plotting exist.

Triggers
~~~~~~~~

- automate_xpcs.py: The initiation script that starts the Automate flow. This is called by the DM workflow with the input imm and hdf5 files as input. The script creates a valid input for Automate based on those input files and starts the flow.
- automate_wait.py: A script to block on an Automate task id until the flow completes. This is used by Suresh to better fit into the DM flow's visualization interface.
- crawl_clutch.py: A script to crawl over Clutch's data and invoke Automate flows.


Flow
----
The flow is started by the automate_xpcs.py script. This generates an input doc and kicks off the flow.
The steps of the flow are as follows.

    1. Transfer (APS->ALCF): Move the hdf5 file from Clutch to Theta's APSDataAnalysis project directory.
    2. Transfer (APS->ALCF): Move the imm file from Clutch to Theta's APSDataAnalysis project directory.
    3. funcX (Theta): Run the Corr function on the two files.
    4. funcX (Theta): Run the xpcs_metadata.py and xpcs_plots.py on the hdf5 file.
    5. funcX (Theta-login): Run Pilot on the resulting data.
    6. (optional) Transfer (ALCF->APS): Return the hdf5 file back to Clutch.
