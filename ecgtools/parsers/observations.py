import pathlib
import traceback
from datetime import datetime

import xarray as xr

from ..builder import INVALID_ASSET, TRACEBACK


def parse_amwg_obs(file):
    """Atmospheric observational data stored in"""
    file = pathlib.Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split('_')
        source = split[0]
        temporal = split[-2]
        if len(split[-2]) == 2:
            month_number = int(temporal)
            time_period = 'monthly'
            temporal = datetime(2020, month_number, 1).strftime('%b').upper()
        else:
            time_period = 'seasonal'

        with xr.open_dataset(file, chunks={}, decode_times=False) as ds:
            for var in ds:
                da = ds[var]
                units = da.units
                long_name = da.long_name
                info = {
                    'source': source,
                    'temporal': temporal,
                    'time_period': time_period,
                    'variable': var,
                    'units': units,
                    'long_name': long_name,
                    'path': str(path),
                }

        return info

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
