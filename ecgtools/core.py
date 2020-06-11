import itertools
import json
from collections import OrderedDict
from pathlib import Path
from typing import List

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
        extension: str = '*.nc',
        depth: int = 0,
        exclude_patterns: list = None,
        global_attrs: list = None,
        parser: callable = None,
        lazy: bool = True,
        nbatches: int = 25,
    ) -> 'Builder':
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
        global_attrs : list
            A list of global attributes to harvest from xarray.Dataset
        parser : callable, optional
            A function (or callable object) that will be called to parse
            attributes from a given file/filepath, by default None
        lazy : bool, optional
            Whether to parse attributes lazily via dask.delayed, by default True
        nbatches : int, optional
            Number of tasks to batch in a single `dask.delayed` call, by default 25

        Raises
        ------
        FileNotFoundError
            When `root_path` does not exist.
        """
        self.root_path = Path(root_path)
        if not self.root_path.is_dir():
            raise FileNotFoundError(f'{root_path} directory does not exist')
        if parser is not None and not callable(parser):
            raise TypeError('parser must be callable.')
        self.dirs = []
        self.filelist = []
        self.df = None
        self.esmcol_data = None
        self.global_attrs = global_attrs or []
        self.parser = parser
        self.lazy = lazy
        self.nbatches = nbatches
        self.extension = extension
        self.depth = depth
        self.exclude_patterns = exclude_patterns or []

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

        except Exception as exc:
            print(exc)

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

    def build(self) -> 'Builder':
        """
        Harvest attributes for a list of files. This method produces a list of dictionaries.
        Each dictionary contains attributes harvested from an individual file.
        """
        if self.filelist:
            if dask.is_dask_collection(self.filelist[0]):
                filelist = self.filelist.compute()
            else:
                filelist = self.filelist
        else:
            filelist = self._get_filelist_from_dirs(lazy=False).filelist
        filepaths = list(itertools.chain(*filelist))
        entries = parse_files_attributes(
            filepaths, self.global_attrs, self.parser, self.lazy, self.nbatches
        )

        if dask.is_dask_collection(entries[0]):
            entries = dask.compute(*entries)[0]
        self.df = pd.DataFrame(entries)
        return self

    def save(
        self,
        catalog_file: str,
        path_column: str,
        variable_column: str,
        data_format: str = None,
        format_column: str = None,
        groupby_attrs: list = None,
        aggregations: List[dict] = None,
        esmcat_version: str = None,
        cat_id: str = None,
        description: str = None,
        attributes: List[dict] = None,
        **kwargs,
    ) -> 'Builder':

        """
        Create a catalog, write it to a comma-separated
        values (CSV) file, and create a corresponding JSON file
        according to ESM collection specification defined at
        https://github.com/NCAR/esm-collection-spec/.

        Parameters
        ----------
        catalog_file : str
           Path to a the CSV file in which catalog contents will be persisted.
        path_column : str
           The name of the column containing the path to the asset.
           Must be in the header of the CSV file.
        variable_column : str
            Name of the attribute column in csv file that contains the variable name.
        data_format : str
            The data format. Valid values are netcdf and zarr.
            If specified, it means that all data in the catalog is the same type.
            Mutually exclusive with `format_column`.
        format_column : str
            The column name which contains the data format, allowing for multiple data assets
            (file formats) types in one catalog. Mutually exclusive with `data_format`.
        groupby_attrs : list
            Column names (attributes) that define data sets that can be aggegrated,
            default None
        aggregations : List[dict]
            List of aggregations to apply to query results, default None
        esmcat_version : str
            The ESM Catalog version the collection implements, default None
        cat_id : str
            Identifier for the collection, default None
        description : str
            Detailed multi-line description to fully explain the collection,
            default None
        attributes : List[dict]
            A list of attributes. An attribute dictionary describes a column
            in the catalog CSV file.
        kwargs : Additional keyword arguments are passed through to the
                 :py:class:`~pandas.DataFrame.to_csv` method.

        Returns
        -------
        `ecgtools.Builder`

        Notes
        -----
        See https://github.com/NCAR/esm-collection-spec/blob/master/collection-spec/collection-spec.md
        for more details on ESM collection specification.

        """

        esmcol_data = OrderedDict()
        if esmcat_version is None:
            esmcat_version = '0.0.1'

        if cat_id is None:
            cat_id = ''

        if description is None:
            description = ''
        esmcol_data['esmcat_version'] = esmcat_version
        esmcol_data['id'] = cat_id
        esmcol_data['description'] = description
        esmcol_data['catalog_file'] = catalog_file
        if attributes is None:
            attributes = []
            for column in self.df.columns:
                attributes.append({'column_name': column, 'vocabulary': ''})

        esmcol_data['attributes'] = attributes
        esmcol_data['assets'] = {'column_name': path_column}
        if (data_format is not None) and (format_column is not None):
            raise ValueError('data_format and format_column are mutually exclusive')

        if data_format is not None:
            esmcol_data['assets']['format'] = data_format
        else:
            esmcol_data['assets']['format_column_name'] = format_column
        if groupby_attrs is None:
            groupby_attrs = [path_column]

        if aggregations is None:
            aggregations = []
        esmcol_data['aggregation_control'] = {
            'variable_column_name': variable_column,
            'groupby_attrs': groupby_attrs,
            'aggregations': aggregations,
        }

        self.esmcol_data = esmcol_data
        json_path = f'{catalog_file.split(".")[0]}.json'
        with open(json_path, mode='w') as outfile:
            json.dump(esmcol_data, outfile, indent=2)

        self.df.to_csv(catalog_file, index=False, **kwargs)
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
