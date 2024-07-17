import argparse
import pathlib
import subprocess
from typing import List, Tuple

from funcx import FuncXExecutor


def glob_xpcs_datasets(
    path: str = None, regex: str = None, extensions: Tuple = None, **data
) -> List[str]:
    """
    Fetch a list of paths on a POSIX filesystem.

    :path str: The POSIX path to search on. The path will be searched recursievly
    :regex str: Filter files returned only if they match the regex. No filtering applied if None.
    :extensions tuple: Filter files by extension (.hdf, .imm, .etc). No filtering is applied if None.
    """
    from pathlib import Path

    def walk(path):
        for p in Path(path).iterdir():
            if p.is_dir():
                yield from walk(p)
                continue
            yield p.resolve()

    datasets = [
        str(p)
        for p in walk(path)
        if (not regex or p.match(regex)) and (not extensions or p.suffix in extensions)
    ]
    metadata_files = []
    for d in datasets:
        hdf_file = None
        dataset_dir = Path(d).parent
        for f in dataset_dir.iterdir():
            if f.suffix == ".hdf":
                hdf_file = f
        metadata_files.append(hdf_file)

    return [(str(d), str(q)) for d, q in zip(datasets, metadata_files) if d and q]


class CollectionTranslator:
    def __init__(self, path, alternate_path=None):
        self.path = pathlib.Path(path)
        self.alternate_path = alternate_path

    def to_posix(self, shared_globus_path: str):
        # assert shared_globus_path.startswith('/'), 'Paths must start at the root share path "/"'
        return self.path / shared_globus_path

    def to_globus(self, posix_path):
        path = pathlib.PosixPath(posix_path)
        try:
            return f"/{str(path.relative_to(self.path))}"
        except ValueError:
            if self.alternate_path:
                return f"/{str(path.relative_to(self.alternate_path))}"
            else:
                raise


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir")
    parser.add_argument("filter")
    parser.add_argument("qmapFile")
    parser.add_argument("experiment")
    parser.add_argument("atype")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ct = CollectionTranslator(
        "/eagle/XPCS-DATA-DYS/", alternate_path="/lus/eagle/projects/XPCS-DATA-DYS/"
    )

    fx = FuncXExecutor(endpoint_id="553e7b64-0480-473c-beef-be762ba979a9")
    future = fx.submit(
        glob_xpcs_datasets,
        ct.to_posix(args.input_dir),
        args.filter,
        [".imm", ".bin", ".h5"],
    )
    result = future.result()

    for inputd, metadata in result:
        dfile, metadata = ct.to_globus(inputd), ct.to_globus(metadata)
        subprocess_args = (
            "dm-start-processing-job",
            "--workflow-name",
            "xpcs8-02-gladier-boost",
            f"filePath:{metadata}",
            f"qmapFile:{args.qmapFile}",
            f"atype:{args.atype}",
            f"experimentName:{args.experiment}",
            f"rawFile:{pathlib.Path(dfile).name}",
        )
        subprocess.run(subprocess_args)
