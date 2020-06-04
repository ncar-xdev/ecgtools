import functools
import itertools
import os
from pathlib import Path

import pytest

from ecgtools import Builder, parse_files_attributes
from ecgtools.core import _parse_file_attributes
from ecgtools.parsers import default_cmip6_ds_parser

here = Path(os.path.dirname(__file__))
cmip6_root_path = here.parent / 'sample_data' / 'cmip' / 'CMIP6'


global_attrs = [
    'activity_id',
    'institution_id',
    'source_id',
    'experiment_id',
    'table_id',
    'frequency',
    'grid_label',
    'realm',
    'variable_id',
    'variant_label',
    'parent_experiment_id',
    'parent_variant_label',
    'sub_experiment',
]
variable_attrs = ['standard_name']

attrs_mapping = {
    'variant_label': 'member_id',
    'parent_variant_label': 'parent_member_id',
}

cmip6_parser = functools.partial(
    default_cmip6_ds_parser, variable_attrs=variable_attrs, attrs_mapping=attrs_mapping
)


def test_builder_invalid_root_path():
    with pytest.raises(FileNotFoundError):
        _ = Builder(root_path='DOES_NOT_EXIST')


@pytest.mark.parametrize('root_path, depth, num_dirs', [(cmip6_root_path, 3, 18)])
def test_builder(root_path, depth, num_dirs):
    b = Builder(root_path, depth=depth)._get_directories()._get_filelist_from_dirs()
    directories = b.dirs
    filelist = b.filelist
    assert isinstance(directories[0], Path)
    assert len(directories) == num_dirs
    assert len(filelist) == num_dirs


@pytest.mark.parametrize('root_path', [cmip6_root_path])
@pytest.mark.parametrize('depth', [3])
@pytest.mark.parametrize('lazy', [False])
def test_cmip6_parser(root_path, depth, lazy):
    b = Builder(
        root_path, depth=depth, extension='*.nc', exclude_patterns=['*/files/*', '*/latest/*'],
    )._get_filelist_from_dirs(lazy=lazy)
    filelist = b.filelist
    filelist = sorted(list(itertools.chain(*filelist)))

    entries_x = parse_files_attributes(filelist, global_attrs, lazy=lazy, parser=cmip6_parser)
    expected = {
        'path': f'{cmip6_root_path}/CMIP/BCC/BCC-CSM2-MR/abrupt-4xCO2/r1i1p1f1/Amon/tasmax/gn/v20181016/tasmax/tasmax_Amon_BCC-CSM2-MR_abrupt-4xCO2_r1i1p1f1_gn_185001-200012.nc',
        'activity_id': 'CMIP',
        'institution_id': 'BCC',
        'source_id': 'BCC-CSM2-MR',
        'experiment_id': 'abrupt-4xCO2',
        'table_id': 'Amon',
        'frequency': 'mon',
        'grid_label': 'gn',
        'realm': 'atmos',
        'variable_id': 'tasmax',
        'parent_experiment_id': 'piControl',
        'sub_experiment': 'none',
        'standard_name': 'air_temperature',
        'dim': '3D',
        'start': '1850-01-16',
        'end': '1850-02-15',
        'version': 'v20181016',
        'member_id': 'r1i1p1f1',
        'parent_member_id': 'r1i1p1f1',
    }
    assert entries_x[0] == expected

    entries_y = b.parse_files_attributes(global_attrs, cmip6_parser, nbatches=1).entries
    assert len(entries_x) == len(entries_y)


def myparser(filepath, global_attrs):
    return {'path': filepath, 'foo': 'bar'}


@pytest.mark.parametrize(
    'filepath, global_attrs, parser, expected',
    [
        (
            f'{cmip6_root_path}/CMIP/BCC/BCC-CSM2-MR/abrupt-4xCO2/r1i1p1f1/Amon/tasmax/gn/v20181016/tasmax/tasmax_Amon_BCC-CSM2-MR_abrupt-4xCO2_r1i1p1f1_gn_185001-200012.nc',
            ['source_id', 'experiment_id', 'table_id', 'grid_label'],
            None,
            {
                'experiment_id': 'abrupt-4xCO2',
                'table_id': 'Amon',
                'source_id': 'BCC-CSM2-MR',
                'grid_label': 'gn',
            },
        ),
        (
            f'{cmip6_root_path}/CMIP/BCC/BCC-CSM2-MR/abrupt-4xCO2/r1i1p1f1/Amon/tasmax/gn/v20181016/tasmax/tasmax_Amon_BCC-CSM2-MR_abrupt-4xCO2_r1i1p1f1_gn_185001-200012.nc',
            ['path', 'foo'],
            myparser,
            {
                'path': f'{cmip6_root_path}/CMIP/BCC/BCC-CSM2-MR/abrupt-4xCO2/r1i1p1f1/Amon/tasmax/gn/v20181016/tasmax/tasmax_Amon_BCC-CSM2-MR_abrupt-4xCO2_r1i1p1f1_gn_185001-200012.nc',
                'foo': 'bar',
            },
        ),
    ],
)
def test_parse_file_attributes(filepath, global_attrs, parser, expected):
    attrs = _parse_file_attributes(filepath, global_attrs, parser)
    for key in global_attrs:
        assert attrs[key] == expected[key]
