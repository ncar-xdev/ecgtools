import os
import pathlib

import pytest

from ecgtools.main import Builder, Directory

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent / 'sample_data'


@pytest.mark.parametrize(
    'path, depth',
    [(str(sample_data_dir / 'cmip' / 'CMIP6'), 1), (str(sample_data_dir / 'cmip' / 'cmip5'), 0)],
)
def test_directory(path, depth):
    directory = Directory(path=path, depth=depth)
    subdirs = directory.subdirs
    assert isinstance(subdirs, list)
    assert len(subdirs) > 0


@pytest.mark.parametrize(
    'paths, depth, storage_options',
    [
        ([str(sample_data_dir / 'cmip' / 'CMIP6' / 'CMIP' / 'BCC')], 2, {}),
        (['s3://ncar-cesm-lens/atm/monthly'], 1, {'anon': True}),
    ],
)
def test_builder_init(paths, depth, storage_options):
    builder = Builder(paths=paths, depth=depth, storage_options=storage_options)
    builder.get_directories()
    assert isinstance(builder.directories, list)
    assert len(builder.directories) > 0


@pytest.mark.parametrize(
    'paths, depth, pattern, storage_options',
    [
        ([str(sample_data_dir / 'cmip' / 'CMIP6' / 'CMIP' / 'BCC')], 2, '**.nc', {}),
        (['s3://ncar-cesm-lens/atm/'], 1, '*.zarr', {'anon': True}),
        (
            ['gs://cmip6/CMIP6/CMIP/BCC/BCC-ESM1/piControl/r1i1p1f1/SImon'],
            1,
            '*/*v????????',
            {'token': 'anon'},
        ),
    ],
)
def test_builder_get_asset_list(paths, depth, pattern, storage_options):
    builder = Builder(paths=paths, depth=depth, storage_options=storage_options)
    builder.get_asset_list(pattern=pattern)
    assert isinstance(builder.asset_list, list)
    assert len(builder.asset_list) > 0
