# XPCS

The Gladier XPCS project leverages the Globus Flows service for rapid data processing
on HPC systems and publication of the results for easy viewing by users. There are 
two major components to this repo: Globus flows for data processing, and portal code
for visualization.

The ``gladier_xpcs/`` package contains all files related to data processing. It contains
all resources for starting and running a flow, in addition to user deployments so
flows can run with ALCF compute resources tied to user allocations. Globus flows are 
composed using the Gladier package.

The ``xpcs_portal/`` package contains all portal-related code for visualizing the
results from successful XPCS flows. In addition, the portal can also start
reprocessing flows for existing datasets which have been published to the portal.
Checkout the [Portal README](./xpcs_portal/README.md) for more information on running
the portal.

## Online Processing

Online processing consists of a Gladier flow run on the talc machine. The core 
flow is located at `gladier_xpcs/flow_boost.py` A script for running the flow with
input can be found in `scripts/xpcs_online_boost_client.py`. In order to run the previous
script, a user needs access to ALCF HPC resources with a running funcx-endpoint.
We track user funcx-endpoints through "deployments", which can be found in
`gladier_xpcs/deployments.py`. 

For more information on running online processing flows, see [online processing](./scripts/online-processing.md).


## Reprocessing

**Note**: Reprocessing is a development feature, and is not enabed for production use.

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
