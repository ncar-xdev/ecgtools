import cf_xarray  # noqa
import xarray as xr


def cmip6_parser(file):
    """Parser for CMIP6"""
    keys = list(
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
                'institution',
                'institution_id',
                'mip_era',
                'nominal_resolution',
                'parent_activity_id',
                'parent_experiment_id',
                'parent_mip_era',
                'parent_source_id',
                'parent_time_units',
                'parient_variant_label',
                'realm',
                'product',
                'source_id',
                'source_type',
                'sub_experiment',
                'sub_experiment_id',
                'table_id',
                'tracking_id',
                'variable_id',
                'variant_label',
            ]
        )
    )
    try:
        ds = xr.open_dataset(file, chunks={}, use_cftime=True)
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

        try:
            vertical_levels = ds[ds.cf['vertical'].name].size
            start_time, end_time = str(ds.cf['T'][0].data), str(ds.cf['T'][-1].data)

        except (KeyError, AttributeError, ValueError):
            pass
        attributes['vertical_levels'] = vertical_levels
        attributes['start_time'] = start_time
        attributes['end_time'] = end_time
        attributes['path'] = file
        return attributes

    except Exception:
        return {}
