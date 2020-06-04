import itertools
from pathlib import Path

import dask
import pandas as pd
import xarray as xr


class Builder:
    """
    Generates a catalog from a list of files.
    """

    def __init__(
        self,
        root_path: str,
        extension: str = None,
        depth: int = None,
        exclude_patterns: list = None,
    ):
        """
        Generate ESM catalog from a list of files.

        Parameters
        ----------
        root_path : str
            Path of root directory.
        extension : str, optional
            File extension, by default None. If None, the builder will look for files with
            "*.nc" extension.
        depth : int, optional
            Recursion depth. Recursively crawl `root_path` up to a specified depth, by default None
        exclude_patterns : list, optional
            Directory, file patterns to exclude during catalog generation, by default None

        Raises
        ------
        FileNotFoundError
            When `root_path` does not exist.
        """
        self.root_path = Path(root_path)
        if not self.root_path.is_dir():
            raise FileNotFoundError(f'{root_path} directory does not exist')
        self.dirs = []
        self.filelist = []
        self.entries = []
        self.df = None
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

    def _get_directories(self, root_path: str = None, depth: int = None):
        """
        Walk `root_path`'s subdirectories and returns a list of directories
        up to the specified depth from `root_path`.

        Parameters
        ----------
        root_path : str, optional
            Root path, by default None
        depth : int, optional
            Recursion depth. Recursively walk `root_path` to a specified depth, by default None

        Returns
        -------
        `ecgtools.Builder`
        """
        if root_path is None:
            root_path = self.root_path

        else:
            root_path = Path(root_path)

        if depth is None:
            depth = self.depth

        pattern = '*/' * (depth + 1)
        dirs = [x for x in root_path.glob(pattern) if x.is_dir()]
        self.dirs = dirs
        return self

    def _get_filelist(self, directory: str, extension: str = None, exclude_patterns: list = None):
        """
        Get a list of all files (with specified extension) under a directory.

        Parameters
        ----------
        directory : str
            Directory path to crawl.
        extension : str, optional
            File extension, by default None
        exclude_patterns : list, optional
            Directory, file patterns to exclude during catalog generation, by default None
        Returns
        -------
        list
          A list of files
        """
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

    _get_filelist_delayed = dask.delayed(_get_filelist)

    def _get_filelist_from_dirs(
        self,
        extension: str = None,
        dirs: list = None,
        depth: int = None,
        exclude_patterns: list = None,
        lazy: bool = True,
    ):
        """
        Get a list of files from a list of directories.

        Parameters
        ----------
        extension : str, optional
            File extension, by default None.
        dirs : list, optional
            An explicit list of directories, by default None
        depth : int, optional
            Recursion depth. Recursively walk root_path to a specified depth, by default None
        exclude_patterns : list, optional
            Directory, file patterns to exclude during catalog generation, by default None
        lazy : bool, optional
            Whether to parse attributes lazily via dask.delayed, by default True

        Returns
        -------
        `ecgtools.Builder`
        """

        if dirs is None:
            if not self.dirs:
                self._get_directories(depth=depth)
                dirs = self.dirs.copy()
            else:
                dirs = self.dirs.copy()
        if lazy:
            filelist = [
                self._get_filelist_delayed(directory, extension, exclude_patterns)
                for directory in dirs
            ]
        else:
            filelist = [
                self._get_filelist(directory, extension, exclude_patterns) for directory in dirs
            ]
        self.filelist = filelist
        return self

    def parse_files_attributes(
        self, global_attrs: list, parser: callable = None, lazy: bool = True, nbatches: int = 25,
    ):
        """
        Harvest attributes for a list of files. This method produces a list of dictionaries.
        Each dictionary contains attributes harvested from an individual file.

        Parameters
        ----------
        global_attrs : list
            A list of global attributes to harvest from xarray.Dataset
        parser : callable, optional
            A function (or callable object) that will be called to parse
            attributes from a given file/filepath, by default None
        lazy : bool, optional
            Whether to parse attributes lazily via dask.delayed, by default True
        nbatches : int, optional
            Number of tasks to batch in a single `dask.delayed` call, by default 25

        Returns
        -------
        `ecgtools.Builder`
        """
        if self.filelist:
            if dask.is_dask_collection(self.filelist[0]):
                filelist = self.filelist.compute()
            else:
                filelist = self.filelist
        else:
            filelist = self._get_filelist_from_dirs(lazy=False).filelist
        filepaths = list(itertools.chain(*filelist))
        self.entries = parse_files_attributes(filepaths, global_attrs, parser, lazy, nbatches)
        return self

    def create_catalog(self, path: str = None, **kwargs):
        """
        Create catalog as a Pandas DataFrame, and write it to a comma-separated
        values (csv) file if `path` is specified.

        Parameters
        ----------
        path : str
          File path, default None.

        **kwargs
           Additional keyword arguments are passed through to the
           :py:class:`~pandas.DataFrame.to_csv` method.

        Returns
        -------
        `ecgtools.Builder`

        """
        self.df = pd.DataFrame(self.entries)
        if path:
            self.df.to_csv(path, **kwargs)
        return self


def parse_files_attributes(
    filepaths: list,
    global_attrs: list,
    parser: callable = None,
    lazy: bool = True,
    nbatches: int = 25,
):
    """
    Harvest attributes for a list of files.

    Parameters
    ----------
    filepaths : list
        an explicit list of files.
    global_attrs : list
        A list of global attributes to harvest from xarray.Dataset
    parser : callable, optional
        A function (or callable object) that will be called to parse
        attributes from a given file/filepath, by default None
    lazy : bool, optional
        Whether to parse attributes lazily via dask.delayed, by default True
    nbatches : int, optional
        Number of tasks to batch in a single `dask.delayed` call, by default 25
    """

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
    """
    Single file attributes harvesting

    Parameters
    ----------
    filepath : str
        Path of a file.
    global_attrs : list
        A list of global attributes to harvest from xarray.Dataset
    parser : callable, optional
        A function (or callable object) that will be called to parse
        attributes from a given file/filepath, by default None, by default None

    Returns
    -------
    dict
        A dictionary of attributes harvested from the input file.
    """

    results = {'path': filepath}
    if parser is not None:
        x = parser(filepath, global_attrs)
        # Merge x and results dictionaries
        results = {**x, **results}
        return results

    else:
        try:
            ds = xr.open_dataset(filepath, decode_times=False, chunks={})
            for attr in global_attrs:
                results[attr] = ds.attrs.get(attr, None)

            return results
        except Exception as e:
            print(e)
            return {}


_parse_file_attributes_delayed = dask.delayed(_parse_file_attributes)
