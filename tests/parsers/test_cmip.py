import pytest

from ecgtools.parsers.cmip import (
    parse_cmip5_using_directories,
    parse_cmip6,
    parse_cmip6_using_directories,
)


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


@pytest.mark.parametrize(
    'file_path',
    [
        'cmip/cmip5/output1/CCCma/CanESM2/esmHistorical/mon/ocnBgchem/Omon/r1i1p1/v20111027/fgco2/fgco2_Omon_CanESM2_esmHistorical_r1i1p1_185001-200512.nc'
    ],
)
def test_parse_cmip5_using_directories(sample_data_directory, file_path):
    path = sample_data_directory / file_path
    entry = parse_cmip5_using_directories(str(path))
    assert {'model', 'variable', 'mip_table'}.issubset(set(list(entry.keys())))
    assert entry['experiment'] == 'esmHistorical'
    assert entry['ensemble_member'] == 'r1i1p1'
    assert entry['mip_table'] == 'Omon'
    assert entry['variable'] == 'fgco2'
