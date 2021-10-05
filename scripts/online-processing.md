# Online Processing (talc setup)

Before continuing, make sure you can run `xpcs_corr_client.py` with your own
deployment in `gladier_xpcs/deployments.py`. If you are not comfortable running
the test flow with that script, seek help before continuing! 

## Talc

### Overview

The Talc machine at the APS is responsible for running the online processing
flow. In effect, it calls the `xpcs_corr_client.py` script using DM, the data
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
eventually passed to the `gladier_xpcs/scripts/xpcs_corr_client.py` script for
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
