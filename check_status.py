import sys
import argparse
from gladier_xpcs.flows import XPCSBoost

class FlowExecutionError(Exception):
    def __init__(self, message="ERROR: Gladier flow failed."):
        self.message = message
        super().__init__(self.message)

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--flowID', required=True)
    return parser.parse_args()

if __name__ == '__main__':
    args = arg_parse()
    client = XPCSBoost()
    status = client.get_status(args.flowID)
    print(f"Status: {status.get('status')}")
    if status.get('status') == "FAILED":
        raise FlowExecutionError(f"ERROR: Gladier flow failed. {status}")
