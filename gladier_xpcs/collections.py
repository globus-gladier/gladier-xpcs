import pathlib


class SharedCollection:

    def __init__(self, uuid, path, name=None):
        self.path = pathlib.Path(path)
        self.uuid = uuid
        self.name = name or self.__class__.__name__

    def to_posix(self, shared_globus_path:str):
        assert str(shared_globus_path).startswith('/'), 'Paths must start at the root share path "/"'
        return self.path / shared_globus_path

    def to_globus(self, posix_path):
        """Takes a full posix path and returns the path to the same location
        relative to the Globus Collection Share Path.

        For example, if the Globus Collection has a share path of /share/solar_data/

        And the path given is /share/solar_data/orbits/pass1.tar.gz

        The result will be:

        /orbits/pass1.tar.gz

        Where /orbits/pass1.tar.gz is the root path to the file when viewed through
        the Globus Shared Collection.
        """
        path = pathlib.PosixPath(posix_path)
        return f'/{str(path.relative_to(self.path))}'
