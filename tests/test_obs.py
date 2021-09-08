import pandas as pd
import pytest

from ecgtools.parsers.observations import parse_amwg_obs

df = pd.DataFrame()


@pytest.mark.parametrize(
    'file_path',
    [
        '/glade/p/cesm/amwg/amwg_diagnostics/obs_data/AIRS_01_climo.nc',
        '/glade/p/cesm/amwg/amwg_diagnostics/obs_data/MODIS_ANN_climo.nc',
    ],
)
def test_obs_parser(file_path):
    parse_dict = parse_amwg_obs(file_path)
    assert isinstance(parse_dict, dict)
    assert isinstance(df.append(parse_dict, ignore_index=True), pd.DataFrame)
