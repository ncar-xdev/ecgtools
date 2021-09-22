import datetime
import enum
import fnmatch
import itertools
import json
import pathlib
import typing
import warnings

import joblib
import pandas as pd
import pydantic

INVALID_ASSET = 'INVALID_ASSET'
TRACEBACK = 'TRACEBACK'


class DataFormatEnum(str, enum.Enum):
    netcdf = 'netcdf'
    zarr = 'zarr'


class Attribute(pydantic.BaseModel):
    column_name: str
    vocabulary: str = None


class Assets(pydantic.BaseModel):
    column_name: str
    format: DataFormatEnum


class Aggregation(pydantic.BaseModel):
    type: str
    attribute_name: str
    options: typing.Optional[typing.Dict[str, typing.Any]]


class AggregationControl(pydantic.BaseModel):
    variable_column_name: str
    groupby_attrs: typing.List[str] = None
    aggregations: typing.List[Aggregation] = None


class ESMCollection(pydantic.BaseModel):
    catalog_file: typing.Union[str, pathlib.Path, pydantic.AnyUrl]
    attributes: typing.List[Attribute]
    assets: Assets
    aggregation_control: AggregationControl
    esmcat_version: str = '0.0.1'
    id: str = None
    description: str = None
    last_updated: typing.Union[datetime.datetime, datetime.date] = None


@pydantic.dataclasses.dataclass
class Builder:
    """
    Generates a catalog from a list of files.

    Parameters
    ----------
    root_path : str or list
        Path(s) of root directory.
    extension : str, optional
        File extension, by default None. If None, the builder will look for files with
        "*.nc" extension.
    depth : int, optional
        Recursion depth. Recursively crawl `root_path` up to a specified depth, by default 0
    exclude_patterns : list, optional
        Directory, file patterns to exclude during catalog generation.
        These could be substring or regular expressions. by default None
    njobs : int, optional
        The maximum number of concurrently running jobs,
        by default -1 meaning all CPUs are used.

    """

    root_path: typing.Union[pydantic.DirectoryPath, typing.List[pydantic.DirectoryPath]]
    extension: str = '.nc'
    depth: int = 0
    exclude_patterns: typing.List[str] = None
    njobs: int = -1
    INVALID_ASSET: typing.ClassVar[str] = INVALID_ASSET
    TRACEBACK: typing.ClassVar[str] = TRACEBACK

    def __post_init_post_parse__(self):
        self.df = pd.DataFrame()
        self.invalid_assets = pd.DataFrame()
        self.dirs = None
        self.filelist = None
        self.entries = None

    def get_directories(self):
        """
        Walk `root_path`'s subdirectories and returns a list of directories
        up to the specified depth from `root_path`.

        Returns
        -------
        `ecgtools.Builder`
        """
        pattern = '*/' * (self.depth + 1)

        if isinstance(self.root_path, pathlib.PosixPath):
            dirs = [x for x in self.root_path.glob(pattern) if x.is_dir()]

        elif isinstance(self.root_path, list):
            dirs = [x for path in self.root_path for x in path.glob(pattern) if x.is_dir()]

        if not dirs:

            if not isinstance(self.root_path, list):
                dirs = [self.root_path]

            else:
                dirs = self.root_path

        self.dirs = dirs
        return self

    def get_filelist(self):
        """Get a list of files from a list of directories."""

        def _filter_files(filelist):
            return not any(
                fnmatch.fnmatch(filelist, pat=exclude_pattern)
                for exclude_pattern in self.exclude_patterns
            )

        def _glob_dir(directory, extension):
            return sorted(list(directory.rglob(f'*{extension}')))

        filelist = joblib.Parallel(n_jobs=self.njobs, verbose=5)(
            joblib.delayed(_glob_dir)(directory, self.extension) for directory in self.dirs
        )
        filelist = itertools.chain(*filelist)
        if self.exclude_patterns:
            filelist = list(filter(_filter_files, filelist))
        self.filelist = sorted(list(filelist))
        return self

    def _parse(self, parsing_func, parsing_func_kwargs=None):
        parsing_func_kwargs = {} if parsing_func_kwargs is None else parsing_func_kwargs
        if parsing_func is None:
            raise ValueError(f'`parsing_func` must a valid Callable. Got {type(parsing_func)}')
        entries = joblib.Parallel(n_jobs=self.njobs, verbose=5)(
            joblib.delayed(parsing_func)(file, **parsing_func_kwargs) for file in self.filelist
        )
        self.entries = entries
        self.df = pd.DataFrame(entries)
        return self

    def clean_dataframe(self):
        if self.INVALID_ASSET in self.df.columns:
            invalid_assets = self.df[self.df[self.INVALID_ASSET].notnull()][
                [self.INVALID_ASSET, self.TRACEBACK]
            ]
            df = self.df[self.df[self.INVALID_ASSET].isnull()].drop(
                columns=[self.INVALID_ASSET, self.TRACEBACK]
            )
            self.invalid_assets = invalid_assets
            if not self.invalid_assets.empty:
                warnings.warn(
                    f'Unable to parse {len(self.invalid_assets)} assets/files. A list of these assets can be found in `.invalid_assets` attribute.',
                    stacklevel=2,
                )
            self.df = df
        return self

    def build(
        self,
        parsing_func: typing.Callable,
        parsing_func_kwargs: dict = None,
        postprocess_func: typing.Callable = None,
    ):
        """Collect a list of files and harvest attributes from them.
        Parameters
        ----------
        parsing_func : callable
            A function that will be called to parse attributes from a given file/filepath
        parsing_func_kwargs: dict, optional
            Additional named arguments passed to `parsing_func`
        postprocess_func: Callable, optional
             A function that will be used to postprocess the built dataframe.

        Returns
        -------
        `ecgtools.Builder`
        """
        self.get_directories().get_filelist()._parse(
            parsing_func, parsing_func_kwargs
        ).clean_dataframe()
        if postprocess_func:
            self.df = postprocess_func(self.df)
        return self

    def save(
        self,
        catalog_file: typing.Union[str, pathlib.Path, pydantic.AnyUrl],
        path_column_name: str,
        variable_column_name: str,
        data_format: DataFormatEnum,
        groupby_attrs: typing.List[str] = None,
        aggregations: typing.List[Aggregation] = None,
        esmcat_version: str = '0.0.1',
        id: str = None,
        description: str = None,
        last_updated: typing.Union[datetime.datetime, datetime.date] = None,
        use_relative_path: bool = True,
        **kwargs,
    ):
        """Persist catalog contents to files.

        Parameters
        ----------
        catalog_file : str
           Path to a the CSV file in which catalog contents will be persisted.
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
        id : str
            Identifier for the collection, default None
        description : str
            Detailed multi-line description to fully explain the collection,
            default None
        use_relative_path: bool
            Whether to use a relative path for the catalog file (csv file)
            entry in the json file, default True
        kwargs : Additional keyword arguments are passed through to the
                 :py:class:`~pandas.DataFrame.to_csv` method.

        Returns
        -------
        `ecgtools.Builder`

        Notes
        -----
        See https://github.com/NCAR/esm-collection-spec/blob/master/collection-spec/collection-spec.md
        for mor

        """
        last_updated = last_updated or datetime.datetime.now().utcnow().strftime(
            '%Y-%m-%dT%H:%M:%SZ'
        )

        for col in {variable_column_name, path_column_name}.union(set(groupby_attrs or [])):
            assert col in self.df.columns, f'{col} must be a column in the dataframe.'

        attributes = [Attribute(column_name=column, vocabulary='') for column in self.df.columns]

        _aggregation_control = AggregationControl(
            variable_column_name=variable_column_name,
            groupby_attrs=groupby_attrs,
            aggregations=aggregations,
        )

        _catalog_file = pathlib.Path(catalog_file)
        catalog_file_location = _catalog_file.name if use_relative_path else str(_catalog_file)
        esmcol_data = ESMCollection(
            catalog_file=catalog_file_location,
            attributes=attributes,
            assets=Assets(column_name=path_column_name, format=data_format),
            aggregation_control=_aggregation_control,
            esmcat_version=esmcat_version,
            id=id,
            description=description,
            last_updated=last_updated,
        )
        esmcol_data = json.loads(esmcol_data.json())
        index = kwargs.pop('index') if 'index' in kwargs else False
        self.df.to_csv(_catalog_file, index=index, **kwargs)
        if not self.invalid_assets.empty:
            invalid_assets_report_file = (
                _catalog_file.parent / f'invalid_assets_{_catalog_file.stem}.csv'
            )
            warnings.warn(
                f'Unable to parse {len(self.invalid_assets)} assets/files. A list of these assets can be found in {invalid_assets_report_file}.',
                stacklevel=2,
            )
            self.invalid_assets.to_csv(invalid_assets_report_file, index=False)
        json_path = _catalog_file.parent / f'{_catalog_file.stem}.json'
        with open(json_path, mode='w') as outfile:
            json.dump(esmcol_data, outfile, indent=2)
        print(f'Saved catalog location: {json_path} and {_catalog_file}')
