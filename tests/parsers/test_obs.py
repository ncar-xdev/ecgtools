import pandas as pd
import pytest

from ecgtools import Builder
from ecgtools.parsers.observations import parse_amwg_obs


@pytest.mark.parametrize(
    'file_path',
    [
        'cesm_obs/AIRS_01_climo.nc',
        'cesm_obs/MODIS_ANN_climo.nc',
    ],
)
def test_obs_parser(sample_data_directory, file_path):
    parsed_dict = parse_amwg_obs(sample_data_directory / file_path)
    assert isinstance(parsed_dict, dict)
    assert 'path' in parsed_dict


@pytest.mark.parametrize(
    'path',
    ['cesm_obs'],
)
def test_obs_builder(sample_data_directory, path):
    b = Builder(paths=[str(sample_data_directory / path)])
    b.build(parsing_func=parse_amwg_obs)
    assert isinstance(b.df, pd.DataFrame)
