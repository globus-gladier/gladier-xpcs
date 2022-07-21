# Online Processing (talc setup)

Before continuing, make sure you can run `xpcs_online_boost_client.py` with your own
deployment in `gladier_xpcs/deployments.py`. If you are not comfortable running
the test flow with that script, seek help before continuing! 

## Setup

Running the xpcs_corr_client.py requires setting up a FuncX endpoint for execution.
Check `gladier_xpcs/deploymets.py` to ensure your globus endpoints and FuncX endpoints are
configured.

### FuncX Endpoint setup for Polaris


Make sure you have two funcx endpoints setup, one for `funcx_endpoint_non_compute` and
`funcx_endpoint_compute` in the `deployments.py` file. Below should serve as a valid
funcx config file for running on Polaris (your `funcx_endpoint_compute `endpoint).
Remember to update the following in your config:

* `worker_init` -- needs to point to your python environment with funcx-endpoint installed
* `container_cmd_options` -- should mount your user directories for using singularity

```
from parsl.addresses import address_by_hostname
from parsl.launchers import AprunLauncher, SingleNodeLauncher
from parsl.providers import CobaltProvider, PBSProProvider
from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor
from funcx_endpoint.strategies import SimpleStrategy

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'polaris': {
        'worker_init': 'source activate my-funcx-endpoint',
        'scheduler_options': '',
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=4,
            # strategy=SimpleStrategy(max_idletime=3600),
            strategy=SimpleStrategy(max_idletime=600),
            address='10.230.2.72', #edtb-02
            scheduler_mode='soft',
            worker_mode='singularity_reuse',
            container_type='singularity',
            container_cmd_options="-H /home/my-username -B /eagle,/projects,/home/my-username",
            provider=PBSProProvider(
                cpus_per_node=32,
                launcher=SingleNodeLauncher(),
                queue='workq',
                scheduler_options=user_opts['polaris']['scheduler_options'],
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['polaris']['worker_init'],
                walltime='12:00:00',
                nodes_per_block=1,
                init_blocks=0,
                min_blocks=0,
                max_blocks=9,
            ),
        )
    ],
)
```

Ensure both your funcx-endpoints are running on Polaris.

```
+----------------+--------------+--------------------------------------+
| Endpoint Name  |    Status    |             Endpoint ID              |
+================+==============+======================================+
| login          | Running      | my-funcx-endpoint-id                 |
+----------------+--------------+--------------------------------------+
| polaris        | Running      | my-other-fx-endpoint-id              |
+----------------+--------------+--------------------------------------+

In the example above, `polaris` uses the FuncX config above, and the endpoint ID
would be used for `funcx_endpoint_compute` in the Glaider deployments.py file.
`funcx_endpoint_non_compute` will be used for login.
```

### Python Requirements

The XPCS flow requires some python dependencies for plotting and publishing metadata.
Install dependencies in the `requirements-tools.txt` file at the top of the repo:

```
pip install -r requirements-tools.txt
pip install -e git+https://github.com/globus-gladier/gladier-xpcs#egg=gladier-xpcs
```

Additionally, Singularity is required for running corr inside a container. Install
with conda:

```
conda install -c conda-forge singularity
```

## Talc

### Overview

The Talc machine at the APS is responsible for running the online processing
flow. In effect, it calls the `xpcs_online_boost_client.py` script using DM, the data
management tool widely used at the APS. An example for a DM call for starting
a flow looks like this:

```
source /home/dm/etc/dm.setup.csh; dm-start-processing-job 
--workflow-name=xpcs8-01-gladier 
filePath:/net/wolf/data/xpcs8/2021-2/akcora202106/cluster_results/A001__Fast_att4_140C_Lq1_001_0001-10000.hdf fileDataDir:A001__Fast_att4_140C_Lq1_001 xpcsGroupName:/xpcs sgeQueueName:xpcs8new.q experimentName:akcora202106
```

This line contains several things to note. First, DM setup is invoked with the
`source /home/dm/etc/dm.setup.csh;` statement. Second a DM workflow is started
using the `--workflow-name=xpcs8-01-gladier` flag. These workflows need to be
configured ahead of time to properly start flows. Third are arguments which are
eventually passed to the `gladier_xpcs/scripts/xpcs_online_boost_client.py` script for
starting the flow. 

### Configuring Workflows

The DB Workflows above are set with the `dm-upsert-workflow` command. This will
register the workflow within DM so that it can be called at a later time. DM
workflows to start Automate flows are located in `scripts/dm`. An example for
setting the xpcs online workflow is below:

```
cd /home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts
dm-upsert-workflow --py-spec dm/workflow-xpcs8-01-gladier.py
```

You should now be able to run the workflow above: 

```
source /home/dm/etc/dm.setup.csh; dm-start-processing-job 
--workflow-name=xpcs8-01-gladier 
filePath:/net/wolf/data/xpcs8/2021-2/akcora202106/cluster_results/A001__Fast_att4_140C_Lq1_001_0001-10000.hdf fileDataDir:A001__Fast_att4_140C_Lq1_001 xpcsGroupName:/xpcs sgeQueueName:xpcs8new.q experimentName:akcora202106
```

Remember to test one before you test many! 

### Reference

Some general notes about Talc:

* Python env: `/home/beams/8IDIUSER/.conda/envs/gladier/bin/python`
* Source Globus Endpoint: [Globus Webapp Link](https://app.globus.org/file-manager?origin_id=fdc7e74a-fa78-11e8-9342-0e3d676669f4&origin_path=%2Fdata%2Fxpcs8%2F)
