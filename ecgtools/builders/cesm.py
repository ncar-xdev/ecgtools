import pathlib

import cf_xarray  # noqa
import xarray as xr

from ..core import extract_attr_with_regex


def smyle_parser(file):
    """Parser for CESM2 Seasonal-to-Multiyear Large Ensemble (SMYLE)"""
    try:
        ds = xr.open_dataset(file, chunks={})
        file = pathlib.Path(file)
        parts = file.parts
        # Case
        case = parts[-6]
        # Extract the component from the file string
        component = parts[-5]

        # Extract the frequency
        frequency = parts[-2]

        date_regex = r'\d{10}-\d{10}|\d{8}-\d{8}|\d{6}-\d{6}'
        date_range = extract_attr_with_regex(parts[-1], date_regex)
        # Pull out the start and end time
        start_time, end_time = date_range.split('-')

        # Extract variable and stream
        y = parts[-1].split(date_range)[0].strip('.').split('.')
        variable = y[-1]
        stream = '.'.join(y[-3:-1])

        # Extract init_year, init_month, member_id
        z = extract_attr_with_regex(case, r'\d{4}-\d{2}.\d{3}').split('.')
        inits = z[0].split('-')
        init_year = int(inits[0])
        init_month = int(inits[1])
        member_id = int(z[-1])

        # Pull out the start and end time
        # start_time, end_time = str(ds.time[0].data), str(ds.time[-1].data)
        lead = ds.time.size

        # Get the long name from dataset
        long_name = ds[variable].attrs.get('long_name', None)

        # Grab the units of the variable
        units = ds[variable].attrs.get('units', None)

        # Set the default of # of vertical levels to 1
        vertical_levels = 1

        try:
            vertical_levels = ds[ds.cf['vertical'].name].size
        except (KeyError, AttributeError, ValueError):
            pass

        # Use standard region names
        regions = {'atm': 'global', 'ocn': 'global_ocean', 'lnd': 'global_land', 'ice': 'global'}

        spatial_domain = regions.get(component, 'global')
        ds.close()

        return {
            'component': component,
            'case': case,
            'variable': variable,
            'long_name': long_name.lower(),
            'frequency': frequency,
            'stream': stream,
            'member_id': member_id,
            'init_year': init_year,
            'init_month': init_month,
            'lead': lead,
            'vertical_levels': vertical_levels,
            'units': units,
            'spatial_domain': spatial_domain,
            'start_time': start_time,
            'end_time': end_time,
            'path': str(file),
        }

    except Exception:
        return {}
