import traceback

import cf_xarray  # noqa
import xarray as xr

from ..builder import INVALID_ASSET, TRACEBACK
from .utilities import extract_attr_with_regex


def parse_cmip6(file):
    """Parser for CMIP6"""
    keys = sorted(
        list(
            set(
                [
                    'activity_id',
                    'branch_method',
                    'branch_time_in_child',
                    'branch_time_in_parent',
                    'experiment',
                    'experiment_id',
                    'frequency',
                    'grid',
                    'grid_label',
                    'institution_id',
                    'nominal_resolution',
                    'parent_activity_id',
                    'parent_experiment_id',
                    'parent_source_id',
                    'parent_time_units',
                    'parent_variant_label',
                    'realm',
                    'product',
                    'source_id',
                    'source_type',
                    'sub_experiment',
                    'sub_experiment_id',
                    'table_id',
                    'variable_id',
                    'variant_label',
                ]
            )
        )
    )

    try:

        with xr.open_dataset(file, chunks={}, use_cftime=True) as ds:
            info = {key: ds.attrs.get(key) for key in keys}
            info['member_id'] = info['variant_label']

            variable_id = info['variable_id']
            if variable_id:
                attrs = ds[variable_id].attrs
                for attr in ['standard_name', 'long_name', 'units']:
                    info[attr] = attrs.get(attr)

            # Set the default of # of vertical levels to 1
            vertical_levels = 1
            start_time, end_time = None, None
            init_year = None
            try:
                vertical_levels = ds[ds.cf['vertical'].name].size
            except (KeyError, AttributeError, ValueError):
                ...

            try:
                start_time, end_time = str(ds.cf['T'][0].data), str(ds.cf['T'][-1].data)
            except (KeyError, AttributeError, ValueError):
                ...
            if info.get('sub_experiment_id'):
                init_year = extract_attr_with_regex(info['sub_experiment_id'], r'\d{4}')
                if init_year:
                    init_year = int(init_year)
            info['vertical_levels'] = vertical_levels
            info['init_year'] = init_year
            info['start_time'] = start_time
            info['end_time'] = end_time
            if not (start_time and end_time):
                info['time_range'] = None
            else:
                info['time_range'] = f'{start_time}-{end_time}'
        info['path'] = str(file)
        info['version'] = (
            extract_attr_with_regex(str(file), regex=r'v\d{4}\d{2}\d{2}|v\d{1}') or 'v0'
        )
        return info

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
