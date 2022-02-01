import os
import pathlib

import intake
import pandas as pd
import pytest

from ecgtools import Builder, RootDirectory, glob_to_regex

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent / 'sample_data'


@pytest.mark.parametrize(
    'path, depth, storage_options,include_patterns, exclude_patterns, num_assets',
    [
        (str(sample_data_dir / 'cmip' / 'CMIP6'), 10, {}, ['*.nc'], [], 59),
        (str(sample_data_dir / 'cmip' / 'cmip5'), 10, {}, ['*.nc'], ['*/esmControl/*'], 27),
        ('s3://ncar-cesm-lens/atm/monthly', 0, {'anon': True}, [], ['*cesmLE-20C*'], 75),
    ],
)
def test_directory(path, depth, storage_options, include_patterns, exclude_patterns, num_assets):
    include_regex, exclude_regex = glob_to_regex(
        include_patterns=include_patterns, exclude_patterns=exclude_patterns
    )
    directory = RootDirectory(
        path=path,
        depth=depth,
        storage_options=storage_options,
        include_regex=include_regex,
        exclude_regex=exclude_regex,
    )
    assets = directory.walk()
    assert len(assets) == num_assets


@pytest.mark.parametrize(
    'paths, depth, storage_options, include_patterns, exclude_patterns, num_assets',
    [
        (
            [
                str(sample_data_dir / 'cmip' / 'CMIP6' / 'CMIP' / 'BCC'),
                str(sample_data_dir / 'cmip' / 'CMIP6' / 'CMIP' / 'IPSL'),
            ],
            8,
            {},
            ['*.nc'],
            [],
            27,
        ),
        (
            ['s3://ncar-cesm-lens/lnd/monthly', 's3://ncar-cesm-lens/ocn/monthly'],
            0,
            {'anon': True},
            [],
            ['*cesmLE-20C*', '*cesmLE-RCP85*'],
            78,
        ),
    ],
)
def test_builder_init(
    paths, depth, storage_options, include_patterns, exclude_patterns, num_assets
):
    builder = Builder(
        paths=paths,
        depth=depth,
        storage_options=storage_options,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )
    builder.get_assets()
    assert isinstance(builder.assets, list)
    assert len(builder.assets) == num_assets


def parsing_func(file):
    return {'path': file, 'variable': 'placeholder'}


def post_process_func(df, times=10):
    df['my_column'] = 1 * times
    return df


@pytest.mark.parametrize(
    'paths, depth, storage_options, include_patterns, exclude_patterns, num_assets',
    [
        (
            [
                str(sample_data_dir / 'cmip' / 'CMIP6' / 'CMIP' / 'BCC'),
                str(sample_data_dir / 'cesm'),
            ],
            1,
            {},
            ['*.nc'],
            [],
            3,
        ),
        (
            ['s3://ncar-cesm-lens/lnd/static', 's3://ncar-cesm-lens/ocn/static'],
            0,
            {'anon': True},
            [],
            ['*cesmLE-20C*', '*cesmLE-RCP85*'],
            4,
        ),
    ],
)
def test_builder_build(
    paths, depth, storage_options, include_patterns, exclude_patterns, num_assets
):
    builder = Builder(
        paths=paths,
        depth=depth,
        storage_options=storage_options,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )
    builder.get_assets()
    assert len(builder.assets) == num_assets
    builder.build(
        parsing_func=parsing_func,
        postprocess_func=post_process_func,
        postprocess_func_kwargs={'times': 100},
    )
    assert isinstance(builder.df, pd.DataFrame)
    assert len(builder.df) == num_assets
    assert set(builder.df.columns) == {'path', 'variable', 'my_column'}


def test_builder_save(tmp_path):
    builder = Builder(
        paths=[str(sample_data_dir / 'cesm')], depth=5, include_patterns=['*.nc']
    ).build(parsing_func=parsing_func)
    builder.save(
        name='test',
        path_column_name='path',
        directory=str(tmp_path),
        data_format='netcdf',
        variable_column_name='variable',
        aggregations=[],
        groupby_attrs=[],
    )

    cat = intake.open_esm_datastore(str(tmp_path / 'test.json'))
    assert isinstance(cat.df, pd.DataFrame)
