import fnmatch
import functools
import typing
import warnings

import fsspec
import joblib
import pandas as pd
import pydantic
import toolz

INVALID_ASSET = 'INVALID_ASSET'
TRACEBACK = 'TRACEBACK'


def _filter(items, *, exclude_patterns):
    return not any(
        fnmatch.fnmatch(items, pat=exclude_pattern) for exclude_pattern in exclude_patterns
    )


class Directory(pydantic.BaseModel):
    path: str
    depth: int = 0
    storage_options: typing.Dict[typing.Any, typing.Any] = pydantic.Field(default_factory=dict)
    exclude_patterns: typing.List[str] = pydantic.Field(default_factory=list)

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

    @property
    def pattern(self):
        return '*/' * (self.depth + 1)

    @property
    def subdirs(self):
        directories = self.mapper.fs.glob(f'{self.raw_path}{self.pattern}', detail=True)
        directories = toolz.dicttoolz.valfilter(
            lambda directory: directory['type'] == 'directory', directories
        )
        valid_directories = list(
            filter(
                functools.partial(_filter, exclude_patterns=self.exclude_patterns),
                directories.keys(),
            )
        )
        valid_directories = [
            Directory(
                path=f'{self.protocol}://{directory}',
                storage_options=self.storage_options,
                exclude_patterns=self.exclude_patterns,
            )
            for directory in valid_directories
        ]
        return valid_directories


@pydantic.dataclasses.dataclass
class Builder:
    paths: list[str]
    storage_options: typing.Dict[typing.Any, typing.Any] = None
    depth: int = 0
    exclude_patterns: list[str] = None
    joblib_parallel_kwargs: typing.Dict[str, typing.Any] = None

    def __post_init_post_parse__(self):
        self.storage_options = self.storage_options or {}
        self.joblib_parallel_kwargs = self.joblib_parallel_kwargs or {}
        self.exclude_patterns = self.exclude_patterns or []
        self._dirs = [
            Directory(
                path=path,
                storage_options=self.storage_options,
                depth=self.depth,
                exclude_patterns=self.exclude_patterns,
            )
            for path in self.paths
        ]
        self.directories = None
        self.asset_list = None
        self.entries = None
        self.df = pd.DataFrame()

    def get_directories(self):
        dirs = [directory.subdirs for directory in self._dirs]
        dirs = list(toolz.unique(toolz.concat(dirs)))
        self.directories = dirs
        return self

    @pydantic.validate_arguments
    def get_asset_list(self, *, pattern: str):
        def _glob_path(directory, pattern):
            return directory.mapper.fs.glob(f'{directory.raw_path}/{pattern}')

        if self.directories is None:
            self.get_directories()
        asset_list = joblib.Parallel(**self.joblib_parallel_kwargs)(
            joblib.delayed(_glob_path)(directory, pattern) for directory in self.directories
        )
        self.asset_list = sorted(
            filter(
                functools.partial(_filter, exclude_patterns=self.exclude_patterns),
                toolz.concat(asset_list),
            )
        )
        return self

    @pydantic.validate_arguments
    def parse(self, *, parsing_func: typing.Callable, parsing_func_kwargs: dict = None):
        if not self.asset_list:
            raise ValueError('asset list provided is None. Please run get_asset_list() first')

        parsing_func_kwargs = {} if parsing_func_kwargs is None else parsing_func_kwargs
        entries = joblib.Parallel(**self.joblib_parallel_kwargs)(
            joblib.delayed(parsing_func)(file, **parsing_func_kwargs) for file in self.asset_list
        )
        self.entries = entries
        self.df = pd.DataFrame(entries)
        return self

    def clean(self):
        if INVALID_ASSET in self.df.columns:
            invalid_assets = self.df[self.df[INVALID_ASSET].notnull()][[INVALID_ASSET, TRACEBACK]]
            df = self.df[self.df[INVALID_ASSET].isnull()].drop(columns=[INVALID_ASSET, TRACEBACK])
            self.invalid_assets = invalid_assets
            if not self.invalid_assets.empty:
                warnings.warn(
                    f'Unable to parse {len(self.invalid_assets)} assets/files. A list of these assets can be found in `.invalid_assets` attribute.',
                    stacklevel=2,
                )
            self.df = df
        return self

    @pydantic.validate_arguments
    def build(
        self,
        *,
        pattern: str,
        parsing_func: typing.Callable,
        parsing_func_kwargs: dict = None,
        postprocess_func: typing.Callable = None,
    ):
        self.get_directories().get_asset_list(pattern=pattern).parse(
            parsing_func=parsing_func, parsing_func_kwargs=parsing_func_kwargs
        ).clean()
        return self
