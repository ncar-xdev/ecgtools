import os
import pathlib
import traceback

import pandas as pd
import pydantic
import pytest

from ecgtools import INVALID_ASSET, TRACEBACK, Builder

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent / 'sample_data'


def parsing_func(file):
    return {'path': file}


def parsing_func_errors(file):
    try:
        file.is_valid()
    except:
        return {INVALID_ASSET: file.as_posix(), TRACEBACK: traceback.format_exc()}


def test_root_path_error():
    with pytest.raises(pydantic.ValidationError):
        Builder('test_directory')


@pytest.mark.parametrize(
    'root_path',
    [
        sample_data_dir / 'cmip' / 'CMIP6',
        sample_data_dir / 'cmip' / 'cmip5',
        sample_data_dir / 'cesm',
    ],
)
def test_init(root_path):
    _ = Builder(root_path)


@pytest.mark.parametrize(
    'root_path',
    [
        sample_data_dir / 'cmip' / 'CMIP6',
        sample_data_dir / 'cmip' / 'cmip5',
        sample_data_dir / 'cesm',
    ],
)
def test_get_filelist(root_path):
    b = Builder(
        root_path,
        exclude_patterns=['*/files/*', '*/latest/*'],
    ).get_directories()
    assert b.dirs
    assert isinstance(b.dirs[0], pathlib.Path)

    b = b.get_filelist()
    assert b.filelist
    assert isinstance(b.filelist[0], pathlib.Path)


def test_parse_error():
    b = Builder(sample_data_dir / 'cesm').get_directories().get_filelist()

    with pytest.raises(ValueError):
        b.parse()


@pytest.mark.parametrize(
    'root_path',
    [
        sample_data_dir / 'cmip' / 'CMIP6',
        sample_data_dir / 'cmip' / 'cmip5',
        sample_data_dir / 'cesm',
    ],
)
def test_parse(root_path):
    b = (
        Builder(root_path, exclude_patterns=['*/files/*', '*/latest/*'], parsing_func=parsing_func)
        .get_directories()
        .get_filelist()
        .parse()
    )
    assert b.entries
    assert isinstance(b.entries[0], dict)
    assert isinstance(b.df, pd.DataFrame)
    assert not b.df.empty


def test_parse_invalid_assets():
    b = (
        Builder(sample_data_dir / 'cesm')
        .get_directories()
        .get_filelist()
        .parse(parsing_func=parsing_func_errors)
        .clean_dataframe()
    )

    assert not b.invalid_assets.empty
    assert set(b.invalid_assets.columns) == set([INVALID_ASSET, TRACEBACK])
