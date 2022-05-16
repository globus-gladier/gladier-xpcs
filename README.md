# XPCS

The Gladier XPCS provides an automation and integration layer for the 8ID beamline at the Advanced Photon Source.

The project leverages the Globus Flows service for rapid data processing
on HPC systems and publication of the results for easy viewing by users. There are two major components to this repo: Globus flows for data processing, and portal code
for visualization.

The ``gladier_xpcs/`` package contains all files related to data processing. It contains
all resources for starting and running a flow, in addition to user deployments so
flows can run with ALCF compute resources tied to user allocations. Globus flows are composed using the Gladier package.

The ``xpcs_portal/`` package contains all globus-portal related code for visualizing the
results from successful XPCS flows. In addition, the portal can also start
reprocessing flows for existing datasets which have been published to the portal.
Checkout the [Portal README](./xpcs_portal/README.md) for more information on running
the portal.

---
## XPCS Flow Client

The main process flow consists of a Gladier flow that moves the data from an endpoint into the HPC resource, applies the Boost package, create graphs and metadata and publishes it into the portal. The core flow is located at `gladier_xpcs/flow_boost.py` A script for running the flow withinput can be found in `scripts/xpcs_corr_client.py`. In order to run the previous script, a user needs access to ALCF HPC resources with a running funcx-endpoint.

### Running the flow

We track user funcx-endpoints through "deployments", which can be found in
`gladier_xpcs/deployments.py`. 

For more information on running online processing flows, see [online processing](./scripts/online-processing.md).

---
## XPCS Reprocessing 

XPCS Reprocessing takes data already published in the portal and re-runs the main flow (above)  with a customized (with a qmap file) hdf file. This flow has an extra step to rename the dataset to publish it under a different title in the portal.

Although scripts exist here to test the reprocessing flow, the actual production flow is
deployed separately on the portal. The portal installs the `gladier_xpcs` package and
imports the Gladier Client.

The main reprocessing client is at `gladier_xpcs/client_reprocess.py`. A script for 
testing reprocessing is located at `scripts/xpcs_reproc_client.py`. Reprocessing
shares some tools with the online processing flow, but contains a handful of custom
tools under `gladier_xpcs/reprocessing_tools`.

### Running the flow

You need to setup your deployment on Theta before you can run reprocessing. This includes
setting up

* a 'login' and 'compute' funcx-endpoint on theta
* a 'processing' directory on theta you have read/write access to

Make sure you are also in the XPCS Developers Globus group to access XPCS datasets which
have already been published.

### Testing the flow
To test a reprocessing flow, ensure Test run a reprocessing flow with the following:

```
cd scripts/
python xpcs_reproc_client.py
```
---
## Deployment

Information related to deploying the flow at 8-ID.
Checklist

Local:

* Install gladier
* Download gladier-xpcs
* Deploy DM scripts if necessary

Remote:
* Install gladier-xpcs
* 

Portal:

*


### Installing the package

```
  conda create -n gladier-xpcs
  conda activate gladier-xpcs

  pip install gladier

  conda install -c nvidia cudatoolkit
  conda install -c pytorch pytorch
  pip install -e git+https://github.com/AZjk/boost_corr#egg=boost_corr
```

### Beamline Setup



### Remote Setup



### Example Config

```
~/.funcx/theta/config.py

from parsl.addresses import address_by_hostname
from parsl.launchers import AprunLauncher
from parsl.providers import CobaltProvider

from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor
from funcx_endpoint.strategies import SimpleStrategy

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'theta': {
        # Add your config here.
        'worker_init': 'source activate funcx',
        'scheduler_options': '',
        # Specify the account/allocation to which jobs should be charged
    }
}


config = Config(
    executors=[
        HighThroughputExecutor(
            heartbeat_period=15,
            heartbeat_threshold=120,
            address=address_by_hostname(),
            scheduler_mode='soft',

            # Set these for using containers
            worker_mode='singularity_reuse',
            container_type='singularity',
            container_cmd_options='-H /home/$USER --bind /eagle/APSDataAnalysis --bind /projects/APSDataAnalysis/',
            provider=CobaltProvider(
                # These may change depending on your allocation
                account='APSDataAnalysis',
                queue='analysis',
                # string to prepend to #COBALT blocks in the submit
                # script to the scheduler eg: '#COBALT -t 50'
                scheduler_options=user_opts['theta']['scheduler_options'],
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate funcx_env'.
                worker_init=user_opts['theta']['worker_init'],
                launcher=AprunLauncher(overrides="-d 64"),
                # Increase this if tasks consistently. outpace available nodes. 
                nodes_per_block=2,
                init_blocks=1,
                min_blocks=0,
                max_blocks=60,
                cmd_timeout=300,
                # 1 hour tends to be a good middleground -- short enough theta
                # usually starts nodes quickly, long enough for (at least lambda)
                # jobs to complete. NOTE: funcx==0.3.3 will not restart tasks that
                # die due to walltime.
                walltime='1:00:00',
            ),
            strategy=SimpleStrategy(max_idletime=900),
            max_workers_per_node=16,
            )
        ]
    )
```