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
        self.builder = Builder(None, parser=self._parser_netcdf, lazy=False)
        self.validater = validater

        # Read in the yaml file and validate
        with open(self.yaml_path, 'r') as f:
            self.input_yaml = yaml.safe_load(f)
        self.valid_yaml = self._validate_yaml()

    def _internal_validation(self):
        """
        Validates the generic yaml input against the schema if the user does not have yamale in
        their environment.

        Parameters
        ----------
        None

        Returns
        -------
        boolean
            True - passes the validation, False - fails the validation
        """

        # verify that we're working with a dictionary
        if not isinstance(self.input_yaml, dict):
            print(
                'ERROR: The experiment/dataset top level is not a dictionary. Make sure you follow the correct format.'
            )
            return False
        # verify that the first line is 'catalog:' and it only appears once in the yaml file
        if len(self.input_yaml.keys()) != 1 or 'catalog' not in self.input_yaml.keys():
            print(
                "ERROR: The first line in the yaml file must be 'catalog:' and it should only appear once."
            )
            return False
        if not isinstance(self.input_yaml['catalog'], list):
            print(
                'ERROR: The catalog entries are not in a list.  make sure you follow the corrrect format.'
            )
            return False
        for dataset in self.input_yaml['catalog']:
            # check to see if there is a data_sources key for each dataset
            if 'data_sources' not in dataset.keys():
                print("ERROR: Each experiment/dataset must have the key 'data_sources'.")
                return False
            # verify that we're working with a list at this level
            if not isinstance(dataset['data_sources'], list):
                print(
                    'ERROR: The data_source entries are not in a list.  Make sure you follow the correct format.'
                )
                return False
            for stream_info in dataset['data_sources']:
                # check to make sure that there's a 'glob_string' key for each data_source
                if 'glob_string' not in stream_info.keys():
                    print("ERROR: Each data_source must contain a 'glob_string' key.")
                    return False
            # ensemble is an option, but we still need to verify that it meets the rules if it is added
            if 'ensemble' in dataset.keys():
                # verify that we're working with a list at this level
                if not isinstance(dataset['ensemble'], list):
                    print(
                        'ERROR: The ensemble entries are not in a list.  Make sure you follow the correct format.'
                    )
                    return False
                for stream_info in dataset['ensemble']:
                    # check to make sure that there's a 'glob_string' key for each ensemble entry
                    if 'glob_string' not in stream_info.keys():
                        print("ERROR: Each ensemble must contain a 'glob_string' key.")
                        return False
        return True

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
                print('Validating yaml file internally.')
                return self._internal_validation()
        else:
            print('Validating yaml file internally.')
            return self._internal_validation()

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
        fileparts['path'] = filepath

        try:
            fileparts['variable'] = []
            # open file
            d = nc.Dataset(filepath, 'r')
            # find what the time (unlimited) dimension is
            dims = list(dict(d.dimensions).keys())
            if 'time' in d.variables.keys():
                times = d['time']
                start = str(times[0])
                end = str(times[-1])
                fileparts['time_range'] = start + '-' + end
            # loop through all variables
            for v in d.variables.keys():
                # add all variables that are not coordinates to the catalog
                if v not in dims:
                    fileparts['variable'].append(v)

            # add the keys that are common just to the particular glob string
            # fileparts.update(local_attrs[filepath])
            for lv in local_attrs[filepath].keys():
                if '<<' in local_attrs[filepath][lv]:
                    for v in fileparts['variable']:
                        if lv not in fileparts.keys():
                            fileparts[lv] = []
                        if hasattr(d.variables[v], lv):
                            fileparts[lv].append(getattr(d.variables[v], lv))
                        else:
                            fileparts[lv].append('None')
                elif '<' in local_attrs[filepath][lv]:
                    k = local_attrs[filepath][lv].replace('<', '').replace('>', '')
                    if hasattr(d, k):
                        fileparts[lv] = getattr(d, k)
                    else:
                        fileparts[lv] = 'None'
                else:
                    fileparts[lv] = local_attrs[filepath][lv]
            # close netcdf file
            d.close()
        except Exception:
            pass
        return fileparts

    def parser(self) -> 'YAML_Parser':
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
                    self.builder.filelist = glob.glob(glob_string)
                    for f in self.builder.filelist:
                        entries[f].update(member)
            # loop over all of the data_sources for the dataset, create a dataframe
            # for each data_source, append that dataframe to a list that will contain
            # the full dataframe (or catalog) based on everything in the yaml file.
            for stream_info in dataset['data_sources']:
                self.builder.filelist = glob.glob(stream_info['glob_string'])
                stream_info.pop('glob_string')
                for f in self.builder.filelist:
                    entries[f].update(stream_info)
                df_parts.append(self.builder.build('path', 'variable', local_attrs=entries).df)
                # create the combined dataframe from all of the data_sources and datasets from
                # the yaml file
                df = pd.concat(df_parts, sort=False)

        self.builder.df = df.sort_values(by=['path'])
        return self.builder
