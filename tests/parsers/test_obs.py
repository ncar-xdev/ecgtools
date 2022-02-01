import os
import pathlib

import pandas as pd
import pytest

from ecgtools import Builder
from ecgtools.parsers.observations import parse_amwg_obs

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent.parent / 'sample_data'


@pytest.mark.parametrize(
    'file_path',
    [
        sample_data_dir / 'cesm_obs' / 'AIRS_01_climo.nc',
        sample_data_dir / 'cesm_obs' / 'MODIS_ANN_climo.nc',
    ],
)
def test_obs_parser(file_path):
    parsed_dict = parse_amwg_obs(file_path)
    assert isinstance(parsed_dict, dict)
    assert 'path' in parsed_dict


@pytest.mark.parametrize(
    'paths',
    [[str(sample_data_dir / 'cesm_obs')]],
)
def test_obs_builder(paths):
    b = Builder(paths=paths)
    b.build(parsing_func=parse_amwg_obs)
    assert isinstance(b.df, pd.DataFrame)
