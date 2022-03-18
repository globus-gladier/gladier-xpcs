"""
XPCS Boost Flow

Summary: This flow executes new xpcs_boost CPU/GPU flow.
- Data is transfered from Clutch to Theta
- An "empty" publication is added to the index
- Nodes are "warmed"
- Boost Multitau and/or Twotime is applied using CPU/GPU
- Plots are made
- Gather + Publish the final data to the portal
"""
import os
import copy
from gladier import GladierBaseClient, generate_flow_definition, utils
from gladier_xpcs.flows.container_flow_base import ContainerBaseClient

# import gladier_xpcs.log  # Uncomment for debug logging


@generate_flow_definition(modifiers={
   'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result[0]'}
})
class XPCSBoost(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    gladier_tools = [
        'gladier_xpcs.tools.TransferFromClutchToTheta',
        'gladier_xpcs.tools.PrePublish',
        'gladier_xpcs.tools.AcquireNodes',
        'gladier_xpcs.tools.BoostCorr',
        'gladier_xpcs.tools.MakeCorrPlots',
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        'gladier_xpcs.tools.Publish',
    ]

    @staticmethod
    def get_flow_input(source_hdf, source_raw_data, source_qmap, deployment, atype='Both', gpu_flag=0,
                       group=None, batch_size='256', verbose=True):
        flow_input = copy.deepcopy(deployment.get_input())

        raw_name = os.path.basename(source_raw_data)
        hdf_name = os.path.basename(source_hdf)
        qmap_name = os.path.basename(source_qmap)
        dataset_name = hdf_name[:hdf_name.rindex('.')] #remove file extension

        dataset_dir = os.path.join(flow_input['input']['staging_dir'], dataset_name)
        # Generate Destination Pathnames.
        raw_file = os.path.join(dataset_dir, 'input', raw_name)
        qmap_file = os.path.join(dataset_dir, 'qmap', qmap_name)
        #do need to transfer the metadata file because corr will look for it
        #internally even though it is not specified as an argument
        input_hdf_file = os.path.join(dataset_dir, 'input', hdf_name)
        output_hdf_file = os.path.join(dataset_dir, 'output', hdf_name)
        # Required by boost_corr to know where to stick the output HDF
        output_dir = os.path.join(dataset_dir, 'output')
        # This tells the corr state where to place version specific info
        execution_metadata_file = os.path.join(dataset_dir, 'execution_metadata.json')

        flow_input['input'].update({

            'boost_corr': {
                    'atype': atype,
                    "qmap": qmap_file,
                    "raw": raw_file,
                    "output": output_dir,
                    "batch_size": 8,
                    "gpu_id": gpu_flag,
                    "verbose": verbose,
                    "masked_ratio_threshold": 0.75,
                    "use_loader": True,
                    "begin_frame": 1,
                    "end_frame": -1,
                    "avg_frame": 1,
                    "stride_frame": 1,
                    "overwrite": False
            },

            'pilot': {
                # This is the directory which will be published
                'dataset': dataset_dir,
                # Old index, switch back to this when we want to publish to the main index
                'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                # Test Index, use this for testing
                # 'index': '2e72452f-e932-4da0-b43c-1c722716896e',
                'project': 'xpcs-8id',
                'source_globus_endpoint': flow_input['input']['globus_endpoint_proc'],
                # Extra groups can be specified here. The XPCS Admins group will always
                # be provided automatically.
                'groups': [group] if group else [],
            },

            'transfer_from_clutch_to_theta_items': [
                {
                    'source_path': source_raw_data,
                    'destination_path': raw_file,
                },
                {
                    'source_path': source_hdf,
                    'destination_path': input_hdf_file,
                },
                {
                    'source_path': source_qmap,
                    'destination_path': qmap_file,
                }
            ],

            'proc_dir': dataset_dir,
            'metadata_file': input_hdf_file,
            'hdf_file': output_hdf_file,
            'execution_metadata_file': execution_metadata_file,

            # # funcX endpoints
            # 'funcx_endpoint_non_compute': deployment_input['input']['funcx_endpoint_non_compute'],
            # 'funcx_endpoint_compute': deployment_input['input']['funcx_endpoint_compute'],

            # # globus endpoints
            'globus_endpoint_clutch': flow_input['input']['globus_endpoint_source'],
            'globus_endpoint_theta': flow_input['input']['globus_endpoint_proc'],
        })
        return flow_input
