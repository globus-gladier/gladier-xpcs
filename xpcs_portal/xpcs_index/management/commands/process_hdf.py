from pprint import pprint
from django.core.management.base import BaseCommand
from concierge_app.models import Bag
from automate_app.funcx import deserialize_payload
from xpcs_index.models import ReprocessingTask

from funcx.sdk.client import FuncXClient


class Command(BaseCommand):
    help = 'Registers a globus compute Function for an Automate Flow'

    def add_arguments(self, parser):
        parser.add_argument('--register', action='store_true', required=False)
        parser.add_argument('--test', required=False)
        parser.add_argument('--payload', required=False, nargs='?',
                            default=None)
        parser.add_argument('--check', action='store_true', required=False)
        parser.add_argument('--output', required=False, nargs='?',
                            default=None)

    def get_task_or_list_all(self, task_id):
        """Get a reprocessing TaskID, or if it does not exist list all tasks"""
        try:
            return ReprocessingTask.objects.get(id=int(task_id))
        except Exception:
            rts = [
                f'Task {t.id}: {t.bag.search_collection.name} -- '
                f'{t.action.status}'
                for t in ReprocessingTask.objects.order_by('id')
            ]
            out = '\n'.join(rts[:10]) or '<No Tasks to display>'
            self.stdout.write(f'Latest tasks: \n{out}')
            return

    def handle(self, *args, **options):
        if options['register']:
            fxc = FuncXClient()
            ep = fxc.register_function(process_hdfs,
                                       description="Process an hdf")
            self.stderr.write(f'FuncX function endpoint has been '
                              f'registered: {ep}')
            self.stderr.write(f'You need to add this somewhere manually!')
        elif options['test']:
            name = options['test']
            if not name:
                self.stderr.write('test needs the name of a search collection')
            bag = Bag.objects.filter(search_collection__name=options['test']
                                     ).first()
            if bag:
                action = ReprocessingTask.new_action(bag, user=bag.user)
                action.save()
                rt = ReprocessingTask(bag=bag, action=action)
                rt.save()
                rt.action.start_flow()
                self.stdout.write(f'Started {action}')
            else:
                bags = [b.search_collection.name for b in Bag.objects.all()]
                self.stderr.write(f'No bag named {options["test"]}, please '
                                  f'use one of the following instead {bags}')
        elif options['check']:
            rts = ReprocessingTask.objects.filter(action__status='ACTIVE')
            if not rts:
                self.stderr.write('No Tasks to update.')
            for rt in rts:
                old = rt.action.status
                rt.action.update_flow()
                self.stdout.write(f'Updated {rt.bag.search_collection.name} '
                                  f'from "{old}" to "{rt.action.status}".')
        elif options.get('payload') is not None:
            raise NotImplementedError('This does not work yet...')
            pl = self.get_task_or_list_all(options['payload']).action.payload
            plain_pl = deserialize_payload(pl['ProcessDataInput']['payload'])
            pprint(plain_pl)
        elif options.get('output') is not None:
            task = self.get_task_or_list_all(options['output'])
            if not task:
                return
            automate_output = task.action.cache['details']['output']
            outputs = [
                data['details']
                for name, data in automate_output.items()
                if 'details' in data
            ]
            for output in outputs:
                if 'result' in output:
                    pprint(deserialize_payload(output['result']))
                elif 'exception' in output:
                    deserialize_payload(output['exception']).reraise()


def process_hdfs(event):
    """Process the hdf file output from a `process_corr` run. The output is
    ready to be placed on petreldata.net."""
    import os
    import XPCS
    from XPCS.tools.xpcs_metadata import gather
    from XPCS.tools.xpcs_plots import make_plots
    from XPCS.tools.xpcs_qc import check_hdf_dataset
    from pilot.client import PilotClient
    staging = event['staging']
    ver = XPCS.xpcs_version
    pc = PilotClient()

    assert pc.context.current == 'xpcs', 'Not in XPCS context!'
    pc.project.current = 'nick-testing'

    skipped = 0
    outputs = {}
    for hdf_dir in os.listdir(staging):
        os.chdir(os.path.join(staging, hdf_dir))
        try:
            outputs[hdf_dir] = {}
            hdf_file = f'{hdf_dir}.hdf'
            if not os.path.exists(hdf_file):
                raise ValueError(f'{hdf_file} does not exist!')
            if check_hdf_dataset(hdf_file) is False:
                skipped += 1

            metadata = gather(hdf_file)
            outputs[hdf_dir]['make_plots'] = make_plots(hdf_file)
            metadata.update(event['custom_metadata'])
            os.chdir(staging)
            outputs[hdf_dir]['pilot'] = pc.upload(
                hdf_dir, '/', metadata=metadata, update=True,
                skip_analysis=True
            )
        except Exception as e:
            outputs[hdf_dir] = str(e)
    return {
        'total': len(os.listdir(staging)),
        'skipped': skipped,
        'outputs': outputs,
        'version': ver,
    }
