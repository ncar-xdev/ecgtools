import itertools
import json
from collections import OrderedDict
from pathlib import Path
from typing import List

import dask
import pandas as pd


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
        self.old_df = None
        self.new_df = None
        self.esmcol_data = None
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
        import fnmatch
        import subprocess

        extension = extension or self.extension
        exclude_patterns = exclude_patterns or self.exclude_patterns
        output = []

        def filter_files(filelist):
            return not any(
                fnmatch.fnmatch(filelist, pat=exclude_pattern)
                for exclude_pattern in exclude_patterns
            )

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
        if self.lazy:
            filelist = [
                self._get_filelist_delayed(directory, extension, exclude_patterns)
                for directory in dirs
            ]
            filelist = dask.compute(*filelist)
        else:
            filelist = [
                self._get_filelist(directory, extension, exclude_patterns) for directory in dirs
            ]

        filelist = list(itertools.chain(*filelist))
        self.filelist = filelist
        return filelist

    def build(
        self,
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
    ) -> 'Builder':
        """
        Harvest attributes for a list of files. This method produces a pandas dataframe
        and a corresponding ESM collection dictionary conforming to ESM collection specification
        defined at https://github.com/NCAR/esm-collection-spec/.

        Parameters
        -----------
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
        """

        esmcol_data = OrderedDict()
        esmcol_data['esmcat_version'] = esmcat_version or '0.0.1'
        esmcol_data['id'] = cat_id or ''
        esmcol_data['description'] = description or ''
        esmcol_data['attributes'] = None

        esmcol_data['assets'] = {'column_name': path_column}
        if (data_format is not None) and (format_column is not None):
            raise ValueError('data_format and format_column are mutually exclusive')

        if data_format is not None:
            esmcol_data['assets']['format'] = data_format
        else:
            esmcol_data['assets']['format_column_name'] = format_column
        groupby_attrs = groupby_attrs or [path_column]
        aggregations = aggregations or []
        esmcol_data['aggregation_control'] = {
            'variable_column_name': variable_column,
            'groupby_attrs': groupby_attrs,
            'aggregations': aggregations,
        }

        filelist = self.filelist or self._get_filelist_from_dirs()
        df = parse_files_attributes(filelist, self.parser, self.lazy, self.nbatches)

        if attributes is None:
            attributes = []
            for column in df.columns:
                attributes.append({'column_name': column, 'vocabulary': ''})

        esmcol_data['attributes'] = attributes
        self.esmcol_data = esmcol_data
        self.df = df
        return self

    def update(self, catalog_file: str, path_column: str) -> 'Builder':
        """
        Update a previously built catalog.

        Parameters
        ----------
        catalog_file : str
           Path to a CSV file for a previously built catalog.
        path_column : str
           The name of the column containing the path to the asset.
           Must be in the header of the CSV file.

        """
        self.old_df = pd.read_csv(catalog_file)
        filelist_from_prev_cat = self.old_df[path_column].tolist()
        filelist = self._get_filelist_from_dirs()
        # Case 1: The new filelist has files that are not included in the
        # Previously built catalog
        files_to_parse = list(set(filelist) - set(filelist_from_prev_cat))
        if files_to_parse:
            self.new_df = parse_files_attributes(
                files_to_parse, self.parser, self.lazy, self.nbatches
            )
        else:
            self.new_df = pd.DataFrame()

        # Case 2: Some files included in the previously built catalog may not existing anymore
        # We need to remove these files in the new version of the catalog
        non_existing_assets = list(set(filelist_from_prev_cat) - set(filelist))
        if non_existing_assets:
            old_df = self.old_df[~self.old_df[path_column].isin(non_existing_assets)].copy()
        else:
            old_df = self.old_df.copy()

        self.df = pd.concat([old_df, self.new_df], ignore_index=True)
        return self

    def save(self, catalog_file: str, **kwargs,) -> 'Builder':

        """
        Create a catalog content to files.

        Parameters
        ----------
        catalog_file : str
           Path to a the CSV file in which catalog contents will be persisted.
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

        self.esmcol_data['catalog_file'] = catalog_file
        json_path = f'{catalog_file.split(".")[0]}.json'
        with open(json_path, mode='w') as outfile:
            json.dump(self.esmcol_data, outfile, indent=2)

        self.df.to_csv(catalog_file, index=False, **kwargs)
        return self


def parse_files_attributes(
    filepaths: list, parser: callable = None, lazy: bool = True, nbatches: int = 25,
) -> pd.DataFrame:
    """
    Harvest attributes for a list of files.

    Parameters
    ----------
    filepaths : list
        an explicit list of files.
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
            sub_results.append(_parse_file_attributes(x, parser))
        return sub_results

    if lazy:
        # Don't Do this: [_parse_file_attributes_delayed(filepath, parser) for filepath in filepaths]
        # It will produce a very large task graph for large collections. For example, CMIP6 archive would results
        # in a task graph with ~1.5 million tasks. To reduce the number of tasks,
        # we will batch multiple tasks into a single task by creating batches of delayed calls.
        results = []
        for i in range(0, len(filepaths), nbatches):
            result_batch = dask.delayed(batch)(filepaths[i : i + nbatches])
            results.append(result_batch)
    else:
        results = [_parse_file_attributes(filepath, parser) for filepath in filepaths]

    if dask.is_dask_collection(results[0]):
        results = dask.compute(*results)
        results = list(itertools.chain(*results))
    return pd.DataFrame(results)


def _parse_file_attributes(filepath: str, parser: callable = None):
    """
    Single file attributes harvesting

    Parameters
    ----------
    filepath : str
        Path of a file.
    parser : callable, optional
        A function (or callable object) that will be called to parse
        attributes from a given file/filepath, by default None, by default None

    Returns
    -------
    df
        A dataframe of attributes harvested from the input file.
    """

    results = {'path': filepath}
    if parser is not None:
        x = parser(filepath)
        # Merge x and results dictionaries
        results = {**x, **results}
    return results


_parse_file_attributes_delayed = dask.delayed(_parse_file_attributes)
