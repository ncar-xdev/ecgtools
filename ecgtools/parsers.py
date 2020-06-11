import re

import xarray as xr


def extract_attr_with_regex(
    input_str: str, regex: str, strip_chars: str = None, ignore_case: bool = True
):

    if ignore_case:
        pattern = re.compile(regex, re.IGNORECASE)
    else:
        pattern = re.compile(regex)
    match = re.findall(pattern, input_str)
    if match:
        match = max(match, key=len)
        if strip_chars:
            match = match.strip(strip_chars)
        else:
            match = match.strip()
        return match
    else:
        return None


def cmip6_default_parser(
    filepath: str,
    global_attrs: list,
    variable_attrs: list = None,
    attrs_mapping: dict = None,
    add_dim: bool = True,
):
    """
    Function that harvests global attributes and variable attributes
    for CMIP6 netCDF output.

    Parameters
    ----------
    filepath : str
        [description]
    global_attrs : list
        global attributes to extract from the netCDF file.
    variable_attrs : list, optional
        variable attributes to extract from the netCDF file, by default None
    attrs_mapping : dict, optional
        A mapping to use to rename some keys/attributes harvested from
        the netCDF file, by default None
    add_dim : bool, optional
        Whether to add variable's dimensionality information to harvested
        attributes, by default True

    Returns
    -------
    dict
        A dictionary of attributes harvested from the input CMIP6 netCDF file.
    """
    try:
        results = {'path': filepath}
        ds = xr.open_dataset(filepath, decode_times=True, use_cftime=True, chunks={})
        g_attrs = ds.attrs
        variable_id = g_attrs['variable_id']
        v_attrs = ds[variable_id].attrs
        for attr in global_attrs:
            results[attr] = g_attrs.get(attr, None)

        if variable_attrs:
            for attr in variable_attrs:
                results[attr] = v_attrs.get(attr, None)

        # Is this a reliable way to get dim?
        results['dim'] = f'{ds[variable_id].data.ndim}D'

        if 'time' in ds.coords:
            times = ds['time']
            start = times[0].dt.strftime('%Y-%m-%d').data.item()
            end = times[-1].dt.strftime('%Y-%m-%d').data.item()
            results['end'] = end
            results['start'] = start

        version_regex = r'v\d{4}\d{2}\d{2}|v\d{1}'
        version = extract_attr_with_regex(filepath, regex=version_regex) or 'v0'
        results['version'] = version

        if attrs_mapping and isinstance(attrs_mapping, dict):
            for old_key, new_key in attrs_mapping.items():
                results[new_key] = results.pop(old_key)

        return results

    except Exception as e:
        # TODO: Record faulty files
        data = {'exception': str(e), 'file': filepath}
        print(data)
        return {}
