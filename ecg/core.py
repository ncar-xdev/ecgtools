import itertools
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
        if not self.root_path.is_dir():
            raise FileNotFoundError(f'{root_path} directory does not exist')
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

    def parse_files_attributes(
        self, global_attrs: list, parser: callable = None, lazy: bool = True, nbatches: int = 25
    ):
        if self.filelist:
            if dask.is_dask_collection(self.filelist[0]):
                filelist = self.filelist.compute()
            else:
                filelist = self.filelist
        else:
            filelist = self.get_filelist_from_dirs(lazy=False)
        filepaths = list(itertools.chain(*filelist))
        entries = parse_files_attributes(filepaths, global_attrs, parser, lazy, nbatches)
        return entries


def parse_files_attributes(
    filepaths: list,
    global_attrs: list,
    parser: callable = None,
    lazy: bool = True,
    nbatches: int = 25,
):
    def batch(seq):
        sub_results = []
        for x in seq:
            sub_results.append(_parse_file_attributes(x, global_attrs, parser))
        return sub_results

    if lazy:
        # Don't Do this: [_parse_file_attributes_delayed(filepath, global_attrs, parser) for filepath in filepaths]
        # It will produce a very large task graph for large collections. For example, CMIP6 archive would results
        # in a task graph with ~1.5 million tasks. To reduce the number of tasks,
        # we will batch multiple tasks into a single task by creating batches of delayed calls.
        results = []
        for i in range(0, len(filepaths), nbatches):
            result_batch = dask.delayed(batch)(filepaths[i : i + nbatches])
            results.append(result_batch)
    else:
        results = [_parse_file_attributes(filepath, global_attrs, parser) for filepath in filepaths]
    return results


def _parse_file_attributes(filepath: str, global_attrs: list, parser: callable = None):

    results = {'path': filepath}
    if parser is not None:
        x = parser(filepath, global_attrs)
        # Merge x and results dictionaries
        results = {**x, **results}
    return results


_parse_file_attributes_delayed = dask.delayed(_parse_file_attributes)
