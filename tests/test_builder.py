import os
import pathlib

import pytest

from ecgtools import Builder, RootDirectory, glob_to_regex

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent / 'sample_data'


@pytest.mark.parametrize(
    'path, depth, include_patterns, exclude_patterns, num_assets',
    [
        (str(sample_data_dir / 'cmip' / 'CMIP6'), 10, ['*.nc'], [], 59),
        (str(sample_data_dir / 'cmip' / 'cmip5'), 10, ['*.nc'], ['*/esmControl/*'], 27),
        ('s3://ncar-cesm-lens/atm/monthly', 0, [], ['*cesmLE-20C*'], 75),
    ],
)
def test_directory(path, depth, include_patterns, exclude_patterns, num_assets):
    include_regex, exclude_regex = glob_to_regex(
        include_patterns=include_patterns, exclude_patterns=exclude_patterns
    )
    directory = RootDirectory(
        path=path, depth=depth, include_regex=include_regex, exclude_regex=exclude_regex
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
