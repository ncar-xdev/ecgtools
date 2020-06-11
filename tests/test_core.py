import functools
import itertools
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import intake
import pandas as pd
import pytest

from ecgtools import Builder
from ecgtools.core import _parse_file_attributes
from ecgtools.parsers import cmip6_default_parser

here = Path(os.path.dirname(__file__))
cmip6_root_path = here.parent / 'sample_data' / 'cmip' / 'CMIP6'


cmip6_global_attrs = [
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
cmip6_variable_attrs = ['standard_name']

cmip6_attrs_mapping = {
    'variant_label': 'member_id',
    'parent_variant_label': 'parent_member_id',
}


cmip6_parser = functools.partial(
    cmip6_default_parser, variable_attrs=cmip6_variable_attrs, attrs_mapping=cmip6_attrs_mapping,
)


def test_builder_invalid_root_path():
    with pytest.raises(FileNotFoundError):
        _ = Builder(root_path='DOES_NOT_EXIST')


def test_builder_invalid_parser():
    with pytest.raises(TypeError):
        _ = Builder(root_path='./', parser='my_func')


@pytest.mark.parametrize(
    'root_path, depth, global_attrs, lazy, parser, expected_df_shape',
    [
        (cmip6_root_path, 3, cmip6_global_attrs, False, cmip6_parser, (59, 19)),
        (cmip6_root_path, 1, cmip6_global_attrs, False, None, (59, 14)),
    ],
)
def test_builder_parser(root_path, depth, global_attrs, lazy, parser, expected_df_shape):
    b = Builder(
        root_path,
        depth=depth,
        extension='*.nc',
        global_attrs=global_attrs,
        exclude_patterns=['*/files/*', '*/latest/*'],
        lazy=lazy,
        parser=parser,
    ).build()

    assert b.df.shape == expected_df_shape
    assert isinstance(b.df, pd.DataFrame)
    assert len(list(itertools.chain(*b.filelist))) == len(b.df)
    intersection = set(global_attrs).intersection(set(b.df.columns))
    assert intersection.issubset(set(global_attrs))


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


@pytest.mark.parametrize('root_path, parser', [(cmip6_root_path, None)])
def test_save(root_path, parser):
    builder = Builder(root_path=root_path)

    with TemporaryDirectory() as local_dir:
        catalog_file = f'{local_dir}/my_catalog.csv'

        builder = builder.build().save(
            catalog_file, 'path', variable_column='variable_id', data_format='netcdf'
        )
        path = f'{local_dir}/my_catalog.json'
        col = intake.open_esm_datastore(path)
        pd.testing.assert_frame_equal(col.df, builder.df)
