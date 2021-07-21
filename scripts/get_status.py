from pprint import pprint
from gladier_xpcs.online_processing import XPCSClient
import argparse


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_id', help='The run for this flow')
    return parser.parse_args()


if __name__ == '__main__':
    args = arg_parse()
    xpcs_client = XPCSClient()
    status = xpcs_client.get_status(args.run_id)
    pprint(status)
