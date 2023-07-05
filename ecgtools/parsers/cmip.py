import pathlib
import traceback

import cf_xarray  # noqa
import numpy as np
import xarray as xr

from ..builder import INVALID_ASSET, TRACEBACK
from .utilities import extract_attr_with_regex, reverse_filename_format


def parse_cmip6(file):
    """Parser for CMIP6"""
    keys = sorted(
        list(
            {
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
            }
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


def parse_cmip6_using_directories(file):
    """
    Extract attributes of a file using information from CMI6 DRS.
    References
    CMIP6 DRS: http://goo.gl/v1drZl
    Controlled Vocabularies (CVs) for use in CMIP6: https://github.com/WCRP-CMIP/CMIP6_CVs
    Directory structure =
    <mip_era>/
        <activity_id>/
            <institution_id>/
                <source_id>/
                    <experiment_id>/
                        <member_id>/
                            <table_id>/
                                <variable_id>/
                                    <grid_label>/
                                        <version>
    file name=<variable_id>_<table_id>_<source_id>_<experiment_id >_<member_id>_<grid_label>[_<time_range>].nc
    For time-invariant fields, the last segment (time_range) above is omitted.
    Example when there is no sub-experiment: tas_Amon_GFDL-CM4_historical_r1i1p1f1_gn_196001-199912.nc
    Example with a sub-experiment: pr_day_CNRM-CM6-1_dcppA-hindcast_s1960-r2i1p1f1_gn_198001-198412.nc
    """
    basename = pathlib.Path(file).name
    filename_template = '{variable_id}_{table_id}_{source_id}_{experiment_id}_{member_id}_{grid_label}_{time_range}.nc'

    gridspec_template = (
        '{variable_id}_{table_id}_{source_id}_{experiment_id}_{member_id}_{grid_label}.nc'
    )
    templates = [filename_template, gridspec_template]
    fileparts = reverse_filename_format(basename, templates=templates)
    try:
        parent = str(pathlib.Path(file).parent)
        parent_split = parent.split(f"/{fileparts['source_id']}/")
        part_1 = parent_split[0].strip('/').split('/')
        grid_label = parent.split(f"/{fileparts['variable_id']}/")[1].strip('/').split('/')[0]
        fileparts['grid_label'] = grid_label
        fileparts['activity_id'] = part_1[-2]
        fileparts['institution_id'] = part_1[-1]
        version_regex = r'v\d{4}\d{2}\d{2}|v\d{1}'
        version = extract_attr_with_regex(parent, regex=version_regex) or 'v0'
        fileparts['version'] = version
        fileparts['path'] = file
        if fileparts['member_id'].startswith('s'):
            fileparts['dcpp_init_year'] = float(fileparts['member_id'].split('-')[0][1:])
            fileparts['member_id'] = fileparts['member_id'].split('-')[-1]
        else:
            fileparts['dcpp_init_year'] = np.nan

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}

    return fileparts


def parse_cmip5_using_directories(file):
    """Extract attributes of a file using information from CMIP5 DRS.
    Notes
    -----
    Reference:
    - CMIP5 DRS: https://pcmdi.llnl.gov/mips/cmip5/docs/cmip5_data_reference_syntax.pdf?id=27
    """

    freq_regex = r'/3hr/|/6hr/|/day/|/fx/|/mon/|/monClim/|/subhr/|/yr/'
    realm_regex = r'aerosol|atmos|land|landIce|ocean|ocnBgchem|seaIce'
    version_regex = r'v\d{4}\d{2}\d{2}|v\d{1}'

    file_basename = str(pathlib.Path(file).name)

    filename_template = (
        '{variable}_{mip_table}_{model}_{experiment}_{ensemble_member}_{temporal_subset}.nc'
    )
    gridspec_template = '{variable}_{mip_table}_{model}_{experiment}_{ensemble_member}.nc'

    templates = [filename_template, gridspec_template]
    fileparts = reverse_filename_format(file_basename, templates)
    frequency = extract_attr_with_regex(file, regex=freq_regex, strip_chars='/')
    realm = extract_attr_with_regex(file, regex=realm_regex)
    version = extract_attr_with_regex(file, regex=version_regex) or 'v0'
    fileparts['frequency'] = frequency
    fileparts['modeling_realm'] = realm
    fileparts['version'] = version
    fileparts['path'] = file
    try:
        part1, part2 = str(pathlib.Path(file).parent).split(fileparts['experiment'])
        part1 = part1.strip('/').split('/')
        fileparts['institute'] = part1[-2]
        fileparts['product_id'] = part1[-3]
    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}

    return fileparts
