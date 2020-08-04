import functools
import glob
import os
import re
from collections import defaultdict
from pathlib import Path

import netCDF4 as nc
import pandas as pd
import xarray as xr

from ecgtools import Builder


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
    global_attrs: list = None,
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


class YAML_Parser:
    """
    Creates a parser that parses a yaml file in order to create a catalog file
    """

    def __init__(
        self, yaml_path: str, csv_path: str = None, validater: str = 'yamale',
    ) -> 'YAML_Parser':
        """
        Get a list of files from a list of directories.

        Parameters
        ----------
        yaml_path : str
            Path to the yaml file to be parsed
        csv_path : str, optional
            Full path to the output csv file
        validater : str, optional
            Choice of yaml validater.  Valid options: 'yamale' or 'internal'; Default: yamale
        """

        import yaml

        self.yaml_path = yaml_path
        self.csv_path = csv_path
        self.builder = None
        self.validater = validater

        # Read in the yaml file and validate
        with open(self.yaml_path, 'r') as f:
            self.input_yaml = yaml.safe_load(f)
        self.valid_yaml = self._validate_yaml()

    def _validate_yaml(self):
        """
        Validates the generic yaml input against the schema.  It uses either yamale or the internal validater.

        Parameters
        ----------
        None

        Returns
        -------
        boolean
            True - passes the validation, False - fails the validation
        """

        # verify the format is correct
        if self.validater == 'yamale':
            try:
                import yamale

                print('Validating yaml file with yamale.')
                cwd = Path(os.path.dirname(__file__))
                schema_path = str(cwd.parent / 'schema') + '/generic_schema.yaml'
                schema = yamale.make_schema(schema_path)
                data = yamale.make_data(self.yaml_path)
                try:
                    yamale.validate(schema, data)
                    print('Validation success! 👍')
                    return True
                except ValueError as e:
                    print(
                        'Yamale found that your file, '
                        + self.yaml_path
                        + ' is not formatted correctly.'
                    )
                    print(e)
                    return False
            except ImportError:
                print('Did not validate yaml because yamale not found.')
                print('If unexpected results occur, try installing yamale and rerun.')
                return True
        else:
            print('Did not validate yaml.')
            print('If unexpected results occur, try installing yamale and rerun.')
            return True

    def _parser_netcdf(self, filepath, local_attrs):
        """
        Opens a netcdf file in order to gather time and requested attribute information.
        Also attaches assigned attributes gathered from the yaml file.

        Parameters
        ----------
        filepath : str
            The full path to the netcdf file to attatch attributes to.
        local_attrs : dict
            Holds attributes that need to be attached to the filenpath.

        Returns
        -------
        dict
            Returns all of the attributes that need to be assigned to the netcdf.
        """

        fileparts = {}

        try:
            fileparts['variable'] = []
            fileparts['start_time'] = []
            fileparts['end_time'] = []
            fileparts['path'] = []

            # open file
            d = nc.Dataset(filepath, 'r')

            # find what the time (unlimited) dimension is
            dims = list(dict(d.dimensions).keys())

            # loop through all variables
            for v in d.variables:
                # add all variables that are not coordinates to the catalog
                if v not in dims:
                    fileparts['variable'].append(v)
                    fileparts['path'].append(filepath)

                    if 'time' in d.variables.keys():
                        times = d['time']
                        fileparts['start_time'].append(times[0])
                        fileparts['end_time'].append(times[-1])

                    # add the keys that are common just to the particular glob string
                    # fileparts.update(local_attrs[filepath])
                    for lv in local_attrs[filepath].keys():
                        if '<<' in local_attrs[filepath][lv]:
                            if lv not in fileparts.keys():
                                fileparts[lv] = []
                            if hasattr(d.variables[v], lv):
                                fileparts[lv].append(getattr(d.variables[v], lv))
                            else:
                                fileparts[lv].append('NaN')
                        elif '<' in local_attrs[filepath][lv]:
                            k = local_attrs[filepath][lv].replace('<', '').replace('>', '')
                            if hasattr(d, k):
                                fileparts[lv] = getattr(d, k)
                            else:
                                fileparts[lv] = 'NaN'
                        else:
                            if lv not in fileparts.keys():
                                fileparts[lv] = []
                            fileparts[lv].append(local_attrs[filepath][lv])
            # close netcdf file
            d.close()
        except Exception:
            pass
        return fileparts

    def parser(self) -> 'Builder':
        """
        Method used to start the parsing process.

        Parameters
        ----------
        None

        Returns
        -------
        Builder
            Returns a Builder object.
        """

        # loop over datasets
        df_parts = []
        entries = defaultdict(dict)
        # for dataset in input_yaml.keys():
        for dataset in self.input_yaml['catalog']:
            # get a list of keys that are common to all files in the dataset
            for g in dataset.keys():
                if 'data_sources' not in g and 'ensemble' not in g:
                    entries['global'] = dataset[g]
            # loop over ensemble members, if they exist
            if 'ensemble' in dataset.keys():
                for member in dataset['ensemble']:
                    glob_string = member.pop('glob_string')
                    filelist = glob.glob(glob_string)
                    for f in filelist:
                        entries[f].update(member)
            # loop over all of the data_sources for the dataset, create a dataframe
            # for each data_source, append that dataframe to a list that will contain
            # the full dataframe (or catalog) based on everything in the yaml file.
            for stream_info in dataset['data_sources']:
                filelist = glob.glob(stream_info['glob_string'])
                stream_info.pop('glob_string')
                for f in filelist:
                    entries[f].update(stream_info)

            partial_parser_netcdf = functools.partial(self._parser_netcdf, local_attrs=entries)
            self.builder = Builder(None, parser=partial_parser_netcdf, lazy=False)
            self.builder.filelist = [x for x in entries.keys() if x != 'global']
            df_parts.append(
                self.builder.build('path', 'variable')
                .df.set_index('path')
                .apply(lambda x: x.apply(pd.Series).stack())
                .reset_index()
                .drop('level_1', 1)
            )
            # create the combined dataframe from all of the data_sources and datasets from
            # the yaml file
            df = pd.concat(df_parts, sort=False)

        self.builder.df = df.sort_values(by=['path'])
        return self.builder
