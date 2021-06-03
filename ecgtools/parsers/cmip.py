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
            attributes = {key: ds.attrs.get(key) for key in keys}
            attributes['member_id'] = attributes['variant_label']

            variable_id = attributes['variable_id']
            if variable_id:
                attrs = ds[variable_id].attrs
                for attr in ['standard_name', 'long_name', 'units']:
                    attributes[attr] = attrs.get(attr)

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
            if attributes.get('sub_experiment_id'):
                init_year = extract_attr_with_regex(attributes['sub_experiment_id'], r'\d{4}')
                if init_year:
                    init_year = int(init_year)
            attributes['vertical_levels'] = vertical_levels
            attributes['init_year'] = init_year
            attributes['start_time'] = start_time
            attributes['end_time'] = end_time
            if not (start_time and end_time):
                attributes['time_range'] = None
            else:
                attributes['time_range'] = f'{start_time}-{end_time}'
            attributes['path'] = file
        return attributes

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
