import fnmatch
import os.path
import re
import tempfile
import typing
import warnings

import fsspec
import joblib
import pandas as pd
import pydantic
import toolz
from intake_esm.cat import (
    Aggregation,
    AggregationControl,
    Assets,
    Attribute,
    DataFormat,
    ESMCatalogModel,
)

INVALID_ASSET = 'INVALID_ASSET'
TRACEBACK = 'TRACEBACK'


def glob_to_regex(*, include_patterns, exclude_patterns):
    include_regex = r'|'.join([fnmatch.translate(x) for x in include_patterns])
    exclude_regex = r'|'.join([fnmatch.translate(x) for x in exclude_patterns]) or r'$.'
    return include_regex, exclude_regex


class RootDirectory(pydantic.BaseModel):
    path: str
    depth: int = 0
    storage_options: typing.Dict[typing.Any, typing.Any] = pydantic.Field(default_factory=dict)
    exclude_regex: str = pydantic.Field(default_factory=str)
    include_regex: str = pydantic.Field(default_factory=str)

    def __hash__(self):
        return hash(f'{self.path}{self.raw_path}')

    @property
    def mapper(self):
        return fsspec.get_mapper(self.path, **self.storage_options)

    @property
    def protocol(self):
        protocol = self.mapper.fs.protocol
        if isinstance(protocol, (list, tuple)):
            protocol = protocol[0]
        return protocol

    @property
    def raw_path(self):
        return self.mapper.fs._strip_protocol(self.path)

    def walk(self):
        all_assets = []
        for root, dirs, files in self.mapper.fs.walk(self.raw_path, maxdepth=self.depth + 1):
            # exclude dirs
            directories = [os.path.join(root, directory) for directory in dirs]
            directories = [
                directory
                for directory in directories
                if not re.match(self.exclude_regex, directory)
            ]

            if files:
                # exclude/include assets
                if self.protocol != 'file':
                    files = [f'{self.protocol}://{os.path.join(root, file)}' for file in files]
                else:
                    files = [os.path.join(root, file) for file in files]
                files = [file for file in files if not re.match(self.exclude_regex, file)]
                files = [file for file in files if re.match(self.include_regex, file)]
                all_assets.extend(files)

            # Look for zarr assets. This works for zarr stores created with consolidated metadata
            # print(all_assets)
            for directory in directories:
                if self.mapper.fs.exists(f'{directory}/.zmetadata'):
                    path = (
                        f'{self.protocol}://{directory}' if self.protocol != 'file' else directory
                    )
                    all_assets.append(path)

        return all_assets


@pydantic.dataclasses.dataclass
class Builder:
    """Generates a catalog from a list of netCDF files or zarr stores

    Parameters
    ----------
    paths : list of str
        List of paths to crawl for assets/files.
    storage_options : dict, optional
        Parameters passed to the backend file-system such as Google Cloud Storage,
        Amazon Web Service S3
    depth : int, optional
        Maximum depth to crawl for assets. Default is 0.
    exclude_patterns : list of str, optional
        List of glob patterns to exclude from crawling.
    include_patterns : list of str, optional
        List of glob patterns to include from crawling.
    joblib_parallel_kwargs : dict, optional
        Parameters passed to joblib.Parallel. Default is {}.
    """

    paths: typing.List[str]
    storage_options: typing.Optional[typing.Dict[typing.Any, typing.Any]] = None
    depth: int = 0
    exclude_patterns: typing.Optional[typing.List[str]] = None
    include_patterns: typing.Optional[typing.List[str]] = None
    joblib_parallel_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None

    def __post_init__(self):
        self.storage_options = self.storage_options or {}
        self.joblib_parallel_kwargs = self.joblib_parallel_kwargs or {}
        self.exclude_patterns = self.exclude_patterns or []
        self.include_patterns = self.include_patterns or []
        # transform glob patterns to regular expressions
        self.include_regex, self.exclude_regex = glob_to_regex(
            include_patterns=self.include_patterns, exclude_patterns=self.exclude_patterns
        )

        self._root_dirs = [
            RootDirectory(
                path=path,
                storage_options=self.storage_options,
                depth=self.depth,
                exclude_regex=self.exclude_regex,
                include_regex=self.include_regex,
            )
            for path in self.paths
        ]
        self.assets = None
        self.invalid_assets = pd.DataFrame()
        self.entries = None
        self.df = pd.DataFrame()

    def get_assets(self):
        assets = [directory.walk() for directory in self._root_dirs]
        self.assets = sorted(toolz.unique(toolz.concat(assets)))
        return self

    @pydantic.validate_arguments
    def parse(
        self, *, parsing_func: typing.Callable, parsing_func_kwargs: typing.Optional[dict] = None
    ):
        if not self.assets:
            raise ValueError('asset list provided is None. Please run `.get_assets()` first')

        parsing_func_kwargs = {} if parsing_func_kwargs is None else parsing_func_kwargs
        entries = joblib.Parallel(**self.joblib_parallel_kwargs)(
            joblib.delayed(parsing_func)(asset, **parsing_func_kwargs) for asset in self.assets
        )
        self.entries = entries
        self.df = pd.DataFrame(entries)
        return self

    def clean_dataframe(self):
        """Clean the dataframe by excluding invalid assets and removing duplicate entries."""
        if INVALID_ASSET in self.df.columns:
            invalid_assets = self.df[self.df[INVALID_ASSET].notnull()][[INVALID_ASSET, TRACEBACK]]
            df = self.df[self.df[INVALID_ASSET].isnull()].drop(columns=[INVALID_ASSET, TRACEBACK])
            self.invalid_assets = invalid_assets
            if not self.invalid_assets.empty:
                warnings.warn(
                    f'Unable to parse {len(self.invalid_assets)} assets. A list of these assets can be found in `.invalid_assets` attribute.',
                    stacklevel=2,
                )
            self.df = df
        return self

    @pydantic.validate_arguments
    def build(
        self,
        *,
        parsing_func: typing.Callable,
        parsing_func_kwargs: typing.Optional[dict] = None,
        postprocess_func: typing.Optional[typing.Callable] = None,
        postprocess_func_kwargs: typing.Optional[dict] = None,
    ):
        """Builds a catalog from a list of netCDF files or zarr stores.

        Parameters
        ----------
        parsing_func : callable
            Function that parses the asset and returns a dictionary of metadata.
        parsing_func_kwargs : dict, optional
            Parameters passed to the parsing function. Default is {}.
        postprocess_func : callable, optional
            Function that post-processes the built dataframe and returns a pandas dataframe.
            Default is None.
        postprocess_func_kwargs : dict, optional
            Parameters passed to the post-processing function. Default is {}.

        Returns
        -------
        :py:class:`~ecgtools.Builder`
            The builder object.

        """
        self.get_assets().parse(
            parsing_func=parsing_func, parsing_func_kwargs=parsing_func_kwargs
        ).clean_dataframe()

        if postprocess_func:
            postprocess_func_kwargs = postprocess_func_kwargs or {}
            self.df = postprocess_func(self.df, **postprocess_func_kwargs)
        return self

    @pydantic.validate_arguments
    def save(
        self,
        *,
        name: str,
        path_column_name: str,
        variable_column_name: str,
        data_format: DataFormat,
        groupby_attrs: typing.Optional[typing.List[str]] = None,
        aggregations: typing.Optional[typing.List[Aggregation]] = None,
        esmcat_version: str = '0.0.1',
        description: typing.Optional[str] = None,
        directory: typing.Optional[str] = None,
        catalog_type: str = 'file',
        to_csv_kwargs: typing.Optional[dict] = None,
        json_dump_kwargs: typing.Optional[dict] = None,
    ):
        """Persist catalog contents to files.

        Parameters
        ----------
        name: str
            The name of the file to save the catalog to.
        path_column_name : str
           The name of the column containing the path to the asset.
           Must be in the header of the CSV file.
        variable_column_name : str
            Name of the attribute column in csv file that contains the variable name.
        data_format : str
            The data format. Valid values are netcdf and zarr.
        aggregations : List[dict]
            List of aggregations to apply to query results, default None
        esmcat_version : str
            The ESM Catalog version the collection implements, default None
        description : str
            Detailed multi-line description to fully explain the collection,
            default None
        directory: str
            The directory to save the catalog to. If None, use the current directory
        catalog_type: str
            The type of catalog to save. Whether to save the catalog table as a dictionary
            in the JSON file or as a separate CSV file. Valid options are 'dict' and 'file'.
        to_csv_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:meth:`~pandas.DataFrame.to_csv` method.
        json_dump_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:func:`~json.dump` function.


        Returns
        -------
        :py:class:`~ecgtools.Builder`
            The builder object.

        Notes
        -----
        See https://github.com/NCAR/esm-collection-spec/blob/master/collection-spec/collection-spec.md
        for more
        """

        for col in {variable_column_name, path_column_name}.union(set(groupby_attrs or [])):
            assert col in self.df.columns, f'{col} must be a column in the dataframe.'

        attributes = [Attribute(column_name=column, vocabulary='') for column in self.df.columns]

        _aggregation_control = AggregationControl(
            variable_column_name=variable_column_name,
            groupby_attrs=groupby_attrs,
            aggregations=aggregations,
        )

        cat = ESMCatalogModel(
            esmcat_version=esmcat_version,
            description=description,
            attributes=attributes,
            aggregation_control=_aggregation_control,
            assets=Assets(column_name=path_column_name, format=data_format),
        )

        cat._df = self.df

        cat.save(
            name=name,
            directory=directory,
            catalog_type=catalog_type,
            to_csv_kwargs=to_csv_kwargs,
            json_dump_kwargs=json_dump_kwargs,
        )

        if not self.invalid_assets.empty:
            invalid_assets_report_file = f'{tempfile.gettempdir()}/{name}_invalid_assets.csv'
            warnings.warn(
                f'Unable to parse {len(self.invalid_assets)} assets/files. A list of these assets can be found in {invalid_assets_report_file}.',
                stacklevel=2,
            )
            self.invalid_assets.to_csv(invalid_assets_report_file, index=False)
