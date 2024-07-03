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
from gladier import GladierBaseClient, generate_flow_definition, utils
from gladier_xpcs.flows.container_flow_base import ContainerBaseClient
from gladier_xpcs.deployments import deployment_map

# import gladier_xpcs.log  # Uncomment for debug logging


@generate_flow_definition(modifiers={
   'publishv2_gather_metadata': {
       'payload': '$.GatherXpcsMetadata.details.results[0].output',
       'WaitTime': 600,
    }
})
class XPCSBoost(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    
    gladier_tools = [
        'gladier_xpcs.tools.SourceTransfer',
        'gladier_xpcs.tools.AcquireNodes',
        'gladier_xpcs.tools.BoostCorr',
        'gladier_xpcs.tools.ResultTransfer',
        'gladier_xpcs.tools.MakeCorrPlots',
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        'gladier_tools.publish.Publishv2',
    ]

    def get_xpcs_input(self, deployment, raw, hdf, qmap, gpu_flag=0, verbose=False, batch_size=256, atype='Both', group=None):
        atype_options = ['Multitau', 'Both'] # "Twotime" is currently not supported!
        if atype not in atype_options:
            raise ValueError(f'Invalid --atype, must be one of: {", ".join(atype_options)}')

        depl_input = deployment.get_input()

        raw_name = os.path.basename(raw)
        hdf_name = os.path.basename(hdf)
        qmap_name = os.path.basename(qmap)
        dataset_name = hdf_name[:hdf_name.rindex('.')] #remove file extension

        dataset_dir = os.path.join(depl_input['input']['staging_dir'], dataset_name)

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

        groups = [self.globus_group]
        if group:
            groups.append(group)
        groups = [f"urn:globus:groups:id:{g}" for g in groups]

        flow_input = {
            'input': {

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
                        "overwrite": True,
                },
                'publishv2': {
                    'dataset': dataset_dir,
                    'destination': '/XPCSDATA/Automate/',
                    'source_collection': deployment.staging_collection.uuid,
                    'source_collection_basepath': str(deployment.staging_collection.path),
                    'destination_collection': str(deployment.pub_collection.uuid),
                    'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                    # Test index
                    # 'index': '2ec9cf61-c0c9-4213-8f1c-452c072c4ccc',
                    'visible_to': groups,

                    # Ingest and Transfer can be disabled for dry-run testing.
                    'enable_publish': True,
                    'enable_transfer': True,

                    'enable_meta_dc': True,
                    'enable_meta_files': True,
                    # Use this to validate the 'dc' or datacite field metadata schema
                    # Requires 'datacite' package
                    # 'metadata_dc_validation_schema': 'schema43',
                    # Custom metadata can be added here.
                    'metadata_file': '/eagle/APSDataProcessing/aps8idi/xpcs_staging/H001_27445_QZ_XPCS_test-01000/output/xpcs_metadata.json',
                },
                'source_transfer': {
                    'source_endpoint_id': deployment.source_collection.uuid,
                    'destination_endpoint_id': deployment.staging_collection.uuid,
                    'transfer_items': [
                        {
                            'source_path': raw,
                            'destination_path': deployment.staging_collection.to_globus(raw_file),
                        },
                        {
                            'source_path': hdf,
                            'destination_path': deployment.staging_collection.to_globus(input_hdf_file),
                        },
                        {
                            'source_path': qmap,
                            'destination_path': deployment.staging_collection.to_globus(qmap_file),
                        }
                    ],
                },

                'proc_dir': dataset_dir,
                'metadata_file': input_hdf_file,
                'hdf_file': output_hdf_file,
                'execution_metadata_file': execution_metadata_file,

                # funcX endpoints
                'login_node_endpoint': depl_input['input']['login_node_endpoint'],
                'compute_endpoint': depl_input['input']['compute_endpoint'],
            }
        }
        return flow_input
