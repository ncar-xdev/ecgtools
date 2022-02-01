import fnmatch
import os.path
import re
import typing
import warnings

import fsspec
import joblib
import pandas as pd
import pydantic
import toolz

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
            dirs[:] = [os.path.join(root, directory) for directory in dirs]
            dirs[:] = [
                directory for directory in dirs if not re.match(self.exclude_regex, directory)
            ]

            if files:
                # exclude/include assets
                files = [os.path.join(root, file) for file in files]
                files = [file for file in files if not re.match(self.exclude_regex, file)]
                files = [file for file in files if re.match(self.include_regex, file)]
                all_assets.extend(files)

            # Look for zarr assets. This works for zarr stores created with consolidated metadata
            # print(all_assets)
            for directory in dirs:
                if self.mapper.fs.exists(f'{directory}/.zmetadata'):
                    all_assets.append(directory)

        return all_assets


@pydantic.dataclasses.dataclass
class Builder:
    paths: typing.List[str]
    storage_options: typing.Dict[typing.Any, typing.Any] = None
    depth: int = 0
    exclude_patterns: typing.List[str] = None
    include_patterns: typing.List[str] = None
    joblib_parallel_kwargs: typing.Dict[str, typing.Any] = None

    def __post_init_post_parse__(self):
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
        self.entries = None
        self.df = pd.DataFrame()

    def get_assets(self):
        assets = [directory.walk() for directory in self._root_dirs]
        self.assets = sorted(toolz.unique(toolz.concat(assets)))
        return self

    @pydantic.validate_arguments
    def parse(self, *, parsing_func: typing.Callable, parsing_func_kwargs: dict = None):
        if not self.assets:
            raise ValueError('asset list provided is None. Please run `.get_assets()` first')

        parsing_func_kwargs = {} if parsing_func_kwargs is None else parsing_func_kwargs
        entries = joblib.Parallel(**self.joblib_parallel_kwargs)(
            joblib.delayed(parsing_func)(asset, **parsing_func_kwargs) for asset in self.assets
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
                    f'Unable to parse {len(self.invalid_assets)} assets/assets. A list of these assets can be found in `.invalid_assets` attribute.',
                    stacklevel=2,
                )
            self.df = df
        return self

    @pydantic.validate_arguments
    def build(
        self,
        *,
        parsing_func: typing.Callable,
        parsing_func_kwargs: dict = None,
        postprocess_func: typing.Callable = None,
    ):
        self.get_assets().parse(
            parsing_func=parsing_func, parsing_func_kwargs=parsing_func_kwargs
        ).clean()
        return self
