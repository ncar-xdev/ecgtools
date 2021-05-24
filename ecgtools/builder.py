import fnmatch
import itertools
import pathlib
import typing

import joblib
import pandas as pd
import pydantic

INVALID_ASSET = 'INVALID_ASSET'
TRACEBACK = 'TRACEBACK'


@pydantic.dataclasses.dataclass
class Builder:
    """
    Generates a catalog from a list of files.

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
    njobs : int, optional
        The maximum number of concurrently running jobs, by default 25

    """

    root_path: pydantic.types.DirectoryPath
    extension: str = '*.nc'
    depth: int = 0
    exclude_patterns: typing.List[str] = None
    parsing_func: typing.Callable = None
    njobs: int = -1

    def __post_init_post_parse__(self):
        self.df = pd.DataFrame()
        self.invalid_assets = pd.DataFrame()
        self.dirs = None
        self.filelist = None
        self.entries = None

    def get_directories(self):
        pattern = '*/' * (self.depth + 1)
        dirs = [x for x in self.root_path.glob(pattern) if x.is_dir()]
        if not dirs:
            dirs = [self.root_path]
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
            return list(directory.rglob(f'{extension}'))

        filelist = joblib.Parallel(n_jobs=self.njobs, verbose=5)(
            joblib.delayed(_glob_dir)(directory, self.extension) for directory in self.dirs
        )
        filelist = itertools.chain(*filelist)
        if self.exclude_patterns:
            filelist = list(filter(_filter_files, filelist))
        self.filelist = list(filelist)
        return self

    def parse(self, parsing_func: typing.Callable = None):
        func = parsing_func or self.parsing_func
        if func is None:
            raise ValueError(f'`parsing_func` must a valid Callable. Got {type(func)}')
        entries = joblib.Parallel(n_jobs=self.njobs, verbose=5)(
            joblib.delayed(func)(file) for file in self.filelist
        )
        self.entries = entries
        self.df = pd.DataFrame(entries)
        return self

    def clean_dataframe(self):
        if INVALID_ASSET in self.df.columns:
            invalid_assets = self.df[self.df[INVALID_ASSET].notnull()][[INVALID_ASSET, TRACEBACK]]
            df = self.df[self.df[INVALID_ASSET].isnull()].drop(columns=[INVALID_ASSET, TRACEBACK])
            self.invalid_assets = invalid_assets
            self.df = df
        return self

    def save(
        self,
        catalog_file: typing.Union[pathlib.Path, str],
        **kwargs,
    ):
        catalog_file = pathlib.Path(catalog_file)
        index = kwargs.pop('index') if 'index' in kwargs else False
        self.df.to_csv(catalog_file, index=index, **kwargs)
        if not self.invalid_assets.empty:
            invalid_assets_report_file = (
                catalog_file.parent / f'invalid_assets_{catalog_file.parts[-1]}'
            )
            self.invalid_assets.to_csv(invalid_assets_report_file, index=False)
        print(f'Saved catalog location: {catalog_file}')

    def build(self):
        self.get_directories().get_filelist().parse().clean_dataframe()
        return self
