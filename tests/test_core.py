import functools
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import intake
import pandas as pd
import pytest
import yaml

from ecgtools import Builder
from ecgtools.parsers import YAMLParser, cmip6_default_parser

here = Path(os.path.dirname(__file__))
cmip6_root_path = here.parent / 'sample_data' / 'cmip' / 'CMIP6'
yaml_root_path = here.parent / 'sample_yaml'

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
    cmip6_default_parser,
    global_attrs=cmip6_global_attrs,
    variable_attrs=cmip6_variable_attrs,
    attrs_mapping=cmip6_attrs_mapping,
)


def test_builder_invalid_root_path():
    with pytest.raises(FileNotFoundError):
        _ = Builder(root_path='DOES_NOT_EXIST')


def test_builder_invalid_parser():
    with pytest.raises(TypeError):
        _ = Builder(root_path='./', parser='my_func')


@pytest.mark.parametrize(
    'root_path, depth, lazy, parser, expected_df_shape',
    [
        (cmip6_root_path, 3, False, cmip6_parser, (59, 19)),
        (cmip6_root_path, 5, True, cmip6_parser, (59, 19)),
        (cmip6_root_path, 0, False, None, (59, 1)),
        (cmip6_root_path, 4, True, None, (59, 1)),
    ],
)
def test_builder_build(root_path, depth, lazy, parser, expected_df_shape):
    b = Builder(
        root_path,
        depth=depth,
        extension='*.nc',
        exclude_patterns=['*/files/*', '*/latest/*'],
        lazy=lazy,
        parser=parser,
    ).build(path_column='path', variable_column='variable_id', data_format='netcdf')

    keys = {'esmcat_version', 'id', 'description', 'attributes', 'assets', 'aggregation_control'}
    assert set(b.esmcol_data.keys()) == keys

    assert b.df.shape == expected_df_shape
    assert isinstance(b.df, pd.DataFrame)
    assert len(b.filelist) == len(b.df)
    intersection = set(cmip6_global_attrs).intersection(set(b.df.columns))
    assert intersection.issubset(set(cmip6_global_attrs))


@pytest.mark.parametrize('root_path, parser', [(cmip6_root_path, None)])
def test_builder_save(root_path, parser):
    builder = Builder(root_path=root_path)

    with TemporaryDirectory() as local_dir:
        catalog_file = f'{local_dir}/my_catalog.csv'

        builder = builder.build(
            path_column='path', variable_column='variable_id', data_format='netcdf'
        ).save(catalog_file)
        path = f'{local_dir}/my_catalog.json'
        col = intake.open_esm_datastore(path)
        pd.testing.assert_frame_equal(col.df, builder.df)
        print(builder.df.shape)


@pytest.mark.parametrize(
    'root_path, parser, num_items, dummy_assets',
    [(cmip6_root_path, None, 30, {}), (cmip6_root_path, None, 59, {'path': 'dummy.nc'})],
)
def test_builder_update(root_path, parser, num_items, dummy_assets):

    with TemporaryDirectory() as local_dir:
        catalog_file = f'{local_dir}/dummy.csv'
        builder = Builder(
            root_path=root_path, exclude_patterns=['*/files/*', '*/latest/*'], parser=parser
        )
        builder = builder.build(
            path_column='path', variable_column='variable_id', data_format='netcdf'
        )
        builder.save(catalog_file)
        df = pd.read_csv(catalog_file).head(num_items)
        if dummy_assets:
            df = df.append(dummy_assets, ignore_index=True)

        df.to_csv(catalog_file, index=False)
        builder = builder.update(catalog_file, path_column='path')
        assert builder.old_df.size == num_items + len(dummy_assets)
        assert (builder.df.size - builder.old_df.size) == builder.new_df.size - len(dummy_assets)


@pytest.mark.parametrize(
    'yaml_path, csv_path, validater, expected_df_shape',
    [
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', (177, 13)),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'foo', (177, 13)),
        (str(yaml_root_path) + '/ensemble.yaml', None, 'yamale', (6612, 11)),
    ],
)
def test_yaml_parser(yaml_path, csv_path, validater, expected_df_shape):
    p = YAMLParser(yaml_path, csv_path=csv_path, validater=validater)
    b = p.parser()

    assert p.valid_yaml
    assert b.df.shape == expected_df_shape
    assert isinstance(b.df, pd.DataFrame)


yinput1 = []
yinput2 = {'foo': []}
yinput3 = {
    'catalog': [
        {
            'ctrl_experiment': 'piControl',
            'ensemble': [
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.001',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.001*.nc',
                    'member_id': '001',
                },
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.002',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.002*.nc',
                    'member_id': '002',
                },
            ],
            'experiment': 'b.e11.BRCP85C5CNBDRD',
        }
    ]
}
yinput4 = {
    'catalog': [
        {'ctrl_experiment': 'piControl', 'data_sources': {}, 'experiment': 'b.e11.BRCP85C5CNBDRD'}
    ]
}
yinput5 = {
    'catalog': [
        {
            'ctrl_experiment': 'piControl',
            'data_sources': [
                {
                    'cell_methods': '<<cell_methods>>',
                    'glob_foo_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.*pop.h.*',
                    'long_name': '<<long_name>>',
                    'model_name': 'pop',
                    'time_freq': '<time_period_freq>',
                    'units': '<<units>>',
                }
            ],
            'ensemble': [
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.001',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.001*.nc',
                    'member_id': '001',
                },
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.002',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.002*.nc',
                    'member_id': '002',
                },
            ],
            'experiment': 'b.e11.BRCP85C5CNBDRD',
        }
    ]
}
yinput6 = {
    'catalog': [
        {
            'ctrl_experiment': 'piControl',
            'data_sources': [
                {
                    'cell_methods': '<<cell_methods>>',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.*pop.h.*',
                    'long_name': '<<long_name>>',
                    'model_name': 'pop',
                    'time_freq': '<time_period_freq>',
                    'units': '<<units>>',
                }
            ],
            'ensemble': {},
            'experiment': 'b.e11.BRCP85C5CNBDRD',
        }
    ]
}
yinput7 = {
    'catalog': [
        {
            'ctrl_experiment': 'piControl',
            'data_sources': [
                {
                    'cell_methods': '<<cell_methods>>',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.*pop.h.*',
                    'long_name': '<<long_name>>',
                    'model_name': 'pop',
                    'time_freq': '<time_period_freq>',
                    'units': '<<units>>',
                }
            ],
            'ensemble': [
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.001',
                    'glob_foo_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.001*.nc',
                    'member_id': '001',
                },
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.002',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.002*.nc',
                    'member_id': '002',
                },
            ],
            'experiment': 'b.e11.BRCP85C5CNBDRD',
        }
    ]
}
yinput8 = {
    'catalog': [
        {
            'ctrl_experiment': 'piControl',
            'data_sources': [
                {
                    'cell_methods': '<<cell_methods>>',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.*pop.h.*',
                    'long_name': '<<long_name>>',
                    'model_name': 'pop',
                    'time_freq': '<time_period_freq>',
                    'units': '<<units>>',
                }
            ],
            'ensemble': [
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.001',
                    'glob_foo_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.001*.nc',
                    'member_id': '001',
                },
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.002',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.002*.nc',
                    'member_id': '002',
                },
            ],
            'experiment': 'b.e11.BRCP85C5CNBDRD',
        }
    ]
}
yinput9 = {
    'catalog': [
        {
            'ctrl_experiment': 'piControl',
            'data_sources': [
                {
                    'cell_methods': '<<cell_methods>>',
                    'glob_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.*pop.h.*',
                    'long_name': '<<long_name>>',
                    'model_name': 'pop',
                    'time_freq': '<time_period_freq>',
                    'units': '<<units>>',
                }
            ],
            'ensemble': [
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.001',
                    'glob_foo_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.001*.nc',
                    'member_id': '001',
                },
                {
                    'experiment_name': 'b.e11.BRCP85C5CNBDRD.f09_g16.002',
                    'glob_foo_string': 'sample_data/cesm-le/b.e11.BRCP85C5CNBDRD.f09_g16.002*.nc',
                    'member_id': '002',
                },
            ],
            'experiment': 'b.e11.BRCP85C5CNBDRD',
        }
    ]
}
yinput10 = {'catalog': {}}


@pytest.mark.parametrize(
    'yaml_path, csv_path, validater, data',
    [
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput1),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput2),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput3),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput4),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput5),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput6),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput7),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput8),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput9),
        (str(yaml_root_path) + '/cmip6.yaml', None, 'yamale', yinput10),
    ],
)
def test_yaml_validation(yaml_path, csv_path, validater, data):

    with TemporaryDirectory() as local_dir:
        temp_yaml_file = f'{local_dir}/my_yaml.yaml'
        with open(temp_yaml_file, 'w') as f:
            yaml.dump(data, f)

        p = YAMLParser(temp_yaml_file, csv_path=csv_path, validater=validater)
        assert bool(p.valid_yaml) is False
