from pathlib import Path

import dask


class Builder:
    def __init__(
        self,
        root_path: str,
        extension: str = None,
        depth: int = None,
        exclude_patterns: list = None,
    ):
        self.root_path = Path(root_path)
        self.dirs = []
        self.filelist = []
        if extension is None:
            self.extension = '*.nc'
        else:
            self.extension = extension

        if depth is None:
            self.depth = 0
        else:
            self.depth = depth

        if exclude_patterns is None:
            self.exclude_patterns = []
        else:
            self.exclude_patterns = exclude_patterns

    def get_directories(self, root_path: str = None, depth: int = None):
        if root_path is None:
            root_path = self.root_path

        else:
            root_path = Path(root_path)

        if depth is None:
            depth = self.depth

        pattern = '*/' * (depth + 1)
        dirs = [x for x in root_path.glob(pattern) if x.is_dir()]
        self.dirs = dirs
        return dirs

    def get_filelist(self, directory: str, extension: str = None, exclude_patterns: list = None):
        import subprocess
        import fnmatch

        def filter_files(filelist):
            return not any(
                fnmatch.fnmatch(filelist, pat=exclude_pattern)
                for exclude_pattern in exclude_patterns
            )

        if extension is None:
            extension = self.extension

        if exclude_patterns is None:
            exclude_patterns = self.exclude_patterns

        output = []
        try:
            cmd = ['find', '-L', directory.as_posix(), '-name', extension]
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            output = proc.stdout.read().decode('utf-8').split()

        except Exception as e:
            print(e)

        finally:
            filelist = list(filter(filter_files, output))
            return filelist

    get_filelist_delayed = dask.delayed(get_filelist)

    def get_filelist_from_dirs(
        self,
        extension: str = None,
        dirs: list = None,
        depth: int = None,
        exclude_patterns: list = None,
        lazy: bool = True,
    ):

        if dirs is None:
            if not self.dirs:
                self.get_directories(depth=depth)
                dirs = self.dirs.copy()
            else:
                dirs = self.dirs.copy()
        if lazy:
            filelist = [
                self.get_filelist_delayed(directory, extension, exclude_patterns)
                for directory in dirs
            ]
        else:
            filelist = [
                self.get_filelist(directory, extension, exclude_patterns) for directory in dirs
            ]
        self.filelist = filelist
        return filelist


def parse_attributes_from_dataset(
    filepaths: list, global_attrs: list, parser: callable = None, lazy: bool = True,
):
    if lazy:
        results = [
            _parse_attributes_from_ds_delayed(filepath, global_attrs, parser)
            for filepath in filepaths
        ]
    else:
        results = [
            _parse_attributes_from_ds(filepath, global_attrs, parser) for filepath in filepaths
        ]
    return results


def _parse_attributes_from_ds(filepath: str, global_attrs: list, parser: callable = None):

    results = {'path': filepath}
    if parser is not None:
        x = parser(filepath, global_attrs)
        results = {**x, **results}
        return results
    else:
        return results


_parse_attributes_from_ds_delayed = dask.delayed(_parse_attributes_from_ds)


def parse_attributes_from_filepath(filepaths: list, lazy: bool = True):
    # TODO: We should migrate Sheri'PR here
    # Ref: https://github.com/NCAR/intake-esm-datastore/pull/70
    ...


def _parse_attributes_from_filepath(filepath):
    # TODO: We should migrate Sheri'PR here
    # Ref: https://github.com/NCAR/intake-esm-datastore/pull/70
    ...


_parse_attributes_from_filepath_delayed = dask.delayed(_parse_attributes_from_filepath)
