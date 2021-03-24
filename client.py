from XPCS.triggers.crawl_clutch import search_dir, get_datasets ##TODO need to figure out where to move crawl_clutch

##XPCS Client Definition
from gladier import GladierBaseClient
from flow_defs import corr_basic_flow_definition, qmap_flow_definition, flow_definition

class XPCS_Corr_Client(GladierBaseClient):
    client_id = 'e6c75d97-532a-4c88-b031-8584a319fa3e'
    gladier_tools = [
        'gladier_tools.xpcs.EigenCorr',
    ]
    flow_definition = corr_basic_flow_definition ##TODO change that to the original flow ryan was using
##

class XPCS_Qmap_Client(GladierBaseClient):
    client_id = 'e6c75d97-532a-4c88-b031-8584a319fa3e'
    gladier_tools = [
        'gladier_tools.xpcs.ApplyQmap',
    ]
    flow_definition = qmap_flow_definition ##TODO change that to the original flow ryan was using
##

##Arg Parsing
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Data to process.",
                        default='/data/xpcs8/2019-1/comm201901/cluster_results/'
                                'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument("--endpoint", help="Source endpoint.", default='fdc7e74a-fa78-11e8-9342-0e3d676669f4')
    parser.add_argument("--imm", help="Path to the imm on the endpoint.",
                        default='/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001'
                                '/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument('--expression', '-e', help='Use Regex to determine datasets to run. '
                                                   'EX: /data/xpcs8/2020-1/foster202003/A38[0-5]_PIB_')
    parser.add_argument("--dry_run", help="If set the flow is not started.",
                        action='store_true')
    parser.add_argument("--block", help="If set, the script will wait until the flow completes.",
                        action='store_true')
    parser.add_argument("--rigaku", help="If set, the corr script use the --rigaku flag.",
                        action='store_true')
    parser.add_argument('--verbose', action='store_true')
    return parser.parse_args()

def check_args():
    
    if args.expression:
        base, exp = os.path.dirname(args.expression), os.path.basename(args.expression)
        print(f'Searching "{base}" with pattern "{exp}"')
        candidates = search_dir(base, pattern=exp)
        print('\n'.join(candidates))
        print(f'\n{len(candidates)} candidates found, generating valid list...')

        datasets = get_datasets([os.path.join(base, c) for c in candidates])
        print(f'INVALID DATASETS:')
        pprint.pprint(datasets['invalid'])
        print(f'VALID DATASETS:')
        pprint.pprint(datasets['valid'])
        # They do not contain a hdf and imm file.
        print(f'({len(datasets["valid"])}/{len(candidates)}) datasets are valid. '
              f'{len(datasets["invalid"])} datasets are invalid.')
        pathnames = datasets['valid_datasets']
    else:
        pathnames = [(args.input, args.imm)]
    return pathnames

if __name__ == '__main__':
    args = parse_args()
    logger = logging.getLogger()
    if args.verbose:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.ERROR)

    corr_cli = XPCS_Client()
    corr_cli.get_input()

    # pathnames = check_args(args)

    # flow_options = None
    # if args.rigaku:
    #     exec_kwargs = {'rigaku': True}
    #     flow_options = {'exec': exec_kwargs}

    # flow_input = corr_cli.create_flow_input(pathnames=pathnames, flow_options=flow_options)

    # if args.dry_run:
    #     print(json.dumps(flow_input, indent=2))
    #     sys.exit()
    # else:
    #     corr_flow = corr_cli.run_flow(flow_input=flow_input)
    #     corr_cli.check_flow(corr_flow['action_id'])
