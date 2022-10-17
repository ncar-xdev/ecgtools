import pytest

from ecgtools.parsers.cmip import parse_cmip6, parse_cmip6_using_directories


@pytest.mark.parametrize(
    'file_path',
    [
        'cmip/CMIP6/CMIP/BCC/BCC-ESM1/piControl/r1i1p1f1/Amon/tasmax/gn/v20181214/tasmax/tasmax_Amon_BCC-ESM1_piControl_r1i1p1f1_gn_185001-230012.nc'
    ],
)
def test_parse_cmip6(sample_data_directory, file_path):
    path = sample_data_directory / file_path
    entry = parse_cmip6(path)
    assert {'activity_id', 'variable_id', 'table_id'}.issubset(set(list(entry.keys())))
    assert entry['experiment_id'] == 'piControl'
    assert entry['member_id'] == 'r1i1p1f1'
    assert entry['grid_label'] == 'gn'
    assert entry['table_id'] == 'Amon'
    assert entry['variable_id'] == 'tasmax'


@pytest.mark.parametrize(
    'file_path',
    [
        'cmip/CMIP6/CMIP/BCC/BCC-ESM1/piControl/r1i1p1f1/Amon/tasmax/gn/v20181214/tasmax/tasmax_Amon_BCC-ESM1_piControl_r1i1p1f1_gn_185001-230012.nc'
    ],
)
def test_parse_cmip6_using_directories(sample_data_directory, file_path):
    path = sample_data_directory / file_path
    entry = parse_cmip6_using_directories(path)
    assert {'activity_id', 'variable_id', 'table_id'}.issubset(set(list(entry.keys())))
    assert entry['experiment_id'] == 'piControl'
    assert entry['member_id'] == 'r1i1p1f1'
    assert entry['grid_label'] == 'gn'
    assert entry['table_id'] == 'Amon'
    assert entry['variable_id'] == 'tasmax'
