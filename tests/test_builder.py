import json
import os
import pathlib
import traceback

import pandas as pd
import pydantic
import pytest

from ecgtools import Builder

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent / 'sample_data'


def parsing_func(file):
    return {'path': file, 'variable': 'placeholder'}


def parsing_func_errors(file):
    try:
        file.is_valid()
    except:
        return {Builder.INVALID_ASSET: file.as_posix(), Builder.TRACEBACK: traceback.format_exc()}


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
def test_build(root_path):
    def func(df):
        df['my_column'] = 'test'
        return df

    b = Builder(
        root_path, exclude_patterns=['*/files/*', '*/latest/*'], parsing_func=parsing_func
    ).build(postprocess_func=func)
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
    assert set(b.invalid_assets.columns) == set([Builder.INVALID_ASSET, Builder.TRACEBACK])


def test_save(tmp_path):
    catalog_file = tmp_path / 'test_catalog.csv'

    b = Builder(sample_data_dir / 'cesm', parsing_func=parsing_func).build()
    b.save(catalog_file, 'path', 'variable', 'netcdf')

    df = pd.read_csv(catalog_file)
    assert len(df) == len(b.df)
    assert set(df.columns) == set(b.df.columns)

    json_path = tmp_path / 'test_catalog.json'
    data = json.load(json_path.open())
    assert set(['catalog_file', 'assets', 'aggregation_control', 'attributes']).issubset(
        set(data.keys())
    )
