import os
import pathlib

import pandas as pd
import pytest

from ecgtools import Builder
from ecgtools.parsers.observations import parse_amwg_obs

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent / 'sample_data'

df = pd.DataFrame()


@pytest.mark.parametrize(
    'file_path',
    [
        sample_data_dir / 'cesm_obs' / 'AIRS_01_climo.nc',
        sample_data_dir / 'cesm_obs' / 'MODIS_ANN_climo.nc',
    ],
)
def test_obs_parser(file_path):
    parse_dict = parse_amwg_obs(file_path)
    assert isinstance(parse_dict, dict)
    assert isinstance(df.append(parse_dict, ignore_index=True), pd.DataFrame)


@pytest.mark.parametrize(
    'file_directory',
    [sample_data_dir / 'cesm_obs'],
)
def test_obs_builder(file_directory):
    b = Builder(file_directory)
    b.build(parse_amwg_obs)
    assert isinstance(b.df, pd.DataFrame)
