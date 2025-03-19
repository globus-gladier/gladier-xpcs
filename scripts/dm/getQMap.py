# Script to pull the qmap file path from the metadata file

import h5py
import argparse

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath", help="Path to the metadata file.", required=True)
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = arg_parse()
    
    with h5py.File(args.filepath) as f:
        print(f.keys())
        for key in f['entry']['instrument']['bluesky']['metadata'].keys():
            try:
                print(f['entry']['instrument']['bluesky']['metadata'].keys())
            except:
                pass
        
