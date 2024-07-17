"""
XPCS Reprocessing

Summary: Data which has already been published on petrel is transferred back into theta,
along with a custom QMap file. The QMap file is applied to the original HDF, and the
new HDF from that changes the behavior of Corr. Corr is then run normally and plots are
created. After that, PublishPreparation will rename the dataset so it doesn't overwrite
the originally published dataset in Globus Search. Some additional reprocessing specific
metadata is added along with standard XPCS metadata. Publish then ingests data into Globus
Search and places data on petrel (probably eagle now), which makes it viewable through
the portal at https://petreldata.alcf.anl.gov
"""
import pathlib
from gladier import GladierBaseClient, generate_flow_definition


@generate_flow_definition(modifiers={
    'gather_xpcs_metadata': {'payload': '$.PublishPreparation.details.results[0].output'},
    'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.results[0].output'}
})
class XPCSReprocessingFlow(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    gladier_tools = [
        # Transfer XPCS files into our processing environment
        'gladier_xpcs.reprocessing_tools.transfer_to_proc.TransferToProc',
        'gladier_xpcs.reprocessing_tools.transfer_qmap.TransferQmap',
        # Apply custom hdf settings to pass info into corr
        'gladier_xpcs.reprocessing_tools.apply_qmap.ApplyQmap',
        'gladier_xpcs.tools.AcquireNodes',
        'gladier_xpcs.tools.EigenCorr',
        'gladier_xpcs.tools.MakeCorrPlots',

        # Change the name of the dataset according to the reprocessing suffix
        # and fetch some extra metadata
        'gladier_xpcs.reprocessing_tools.publish_preparation.PublishPreparation',
        # Gather XPCS specific metadata from the HDF file
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        # Publish using pilot to the Globus Search index
        'gladier_xpcs.tools.Publish',
    ]

    @staticmethod
    def get_xpcs_input(deployment, hdf_source, imm_source, qmap_source):
        """
        This is a special method which builds runtime input for files that should
        be computed on theta from the source files and the deployment locations.
        The deployment locations (primarily proc_dir), dictates the path on the
        compute endpoint which most globus compute functions will use when opening the actual
        files.
        """
        dep_input = deployment.get_input()['input']
        flow_input = super().get_input()
        flow_input['input'].update(dep_input)

        hdf_source = pathlib.Path(hdf_source)
        imm_source = pathlib.Path(imm_source)
        qmap_source = pathlib.Path(qmap_source)
        staging_dir = pathlib.Path(dep_input['staging_dir'])

        hdf_file = staging_dir / hdf_source.parent.name / hdf_source.name
        imm_file = staging_dir / hdf_source.parent.name / imm_source.name
        qmap_file = staging_dir / hdf_source.parent.name / qmap_source.name

        flow_input['input'].update({
            'pilot': {
                # This is the directory which will be published to petrel
                'dataset': str(hdf_file.parent),
                'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                'project': 'xpcs-8id',
                'source_globus_endpoint': dep_input['globus_endpoint_proc'],
                # Extra groups can be specified here. The XPCS Admins group will always
                # be provided automatically.
                'groups': dep_input.get('groups', []),
            },
            'hdf_file_source': str(hdf_source),
            'imm_file_source': str(imm_source),
            'proc_dir': str(hdf_file.parent),
            'hdf_file': str(hdf_file),
            'imm_file': str(imm_file),
            'qmap_file': str(qmap_file),
        })
        return flow_input

    def get_label(self, flow_input):
        return str(pathlib.Path(flow_input['input']['proc_dir']).name)[:62]
