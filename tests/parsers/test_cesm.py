import pytest

from ecgtools.parsers.cesm import parse_cesm_timeseries


@pytest.mark.parametrize(
    'file_path, variable, frequency, component, case',
    [
        (
            'cesm-le/b.e11.B1850C5CN.f09_g16.005.pop.h.SHF.040001-049912.nc',
            'SHF',
            'month_1',
            'ocn',
            'b.e11.B1850C5CN.f09_g16.005',
        ),
        (
            'cesm/g.e11_LENS.GECOIAF.T62_g16.009.pop.h.ECOSYS_XKW.024901-031612.nc',
            'ECOSYS_XKW',
            'month_1',
            'ocn',
            'g.e11_LENS.GECOIAF.T62_g16.009',
        ),
        (
            'cesm/g.e11_LENS.GECOIAF.T62_g16.009.pop.h.ECOSYS_XKW.024901-031612.nc',
            'ECOSYS_XKW',
            'month_1',
            'ocn',
            'g.e11_LENS.GECOIAF.T62_g16.009',
        ),
        (
            'cesm/g.e11_LENS.GECOIAF.T62_g16.009.pop.h.ECOSYS_XKW.024901-031612.nc',
            'ECOSYS_XKW',
            'month_1',
            'ocn',
            'g.e11_LENS.GECOIAF.T62_g16.009',
        ),
        (
            'cesm/g.e11_LENS.GECOIAF.T62_g16.009.pop.h.ECOSYS_XKW.024901-031612.nc',
            'ECOSYS_XKW',
            'month_1',
            'ocn',
            'g.e11_LENS.GECOIAF.T62_g16.009',
        ),
        (
            'cesm/g.e11_LENS.GECOIAF.T62_g16.009.pop.h.ECOSYS_XKW.024901-031612.nc',
            'ECOSYS_XKW',
            'month_1',
            'ocn',
            'g.e11_LENS.GECOIAF.T62_g16.009',
        ),
        (
            'cesm/g.e11_LENS.GECOIAF.T62_g16.009.pop.h.ECOSYS_XKW.024901-031612.nc',
            'ECOSYS_XKW',
            'month_1',
            'ocn',
            'g.e11_LENS.GECOIAF.T62_g16.009',
        ),
    ],
)
def test_cesm_timeseries(sample_data_directory, file_path, variable, frequency, component, case):
    path = sample_data_directory / file_path
    entry = parse_cesm_timeseries(path)
    assert {
        'component',
        'stream',
        'case',
        'member_id',
        'variable',
        'start_time',
        'end_time',
        'time_range',
        'long_name',
        'units',
        'vertical_levels',
        'frequency',
        'path',
    } == set(entry.keys())
    assert entry['variable'] == variable
    assert entry['frequency'] == frequency
    assert entry['path'] == str(path)
    assert entry['case'] == case
    assert entry['component'] == component
