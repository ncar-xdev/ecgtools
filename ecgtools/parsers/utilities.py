import itertools
import pathlib
import re
import subprocess
from functools import lru_cache

import dask
from intake.source.utils import reverse_format


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
        match = match.strip(strip_chars) if strip_chars else match.strip()
        return match
    else:
        return None


def reverse_filename_format(filename, templates):
    """
    Uses intake's ``reverse_format`` utility to reverse the string method format.
    Given format_string and resolved_string, find arguments
    that would give format_string.format(arguments) == resolved_string
    """
    x = {}

    for template in templates:
        try:
            x = reverse_format(template, filename)
            if x:
                break
        except ValueError:
            continue
    if not x:
        print(f'Failed to parse file: {filename} using patterns: {templates}')
    return x


@lru_cache(maxsize=None)
def get_asset_list(root_path, depth=0, extension='*.nc'):
    from dask.diagnostics import ProgressBar

    root = pathlib.Path(root_path)
    pattern = '*/' * (depth + 1)

    dirs = [x for x in root.glob(pattern) if x.is_dir()]

    @dask.delayed
    def _file_dir_files(directory):
        try:
            cmd = ['find', '-L', directory.as_posix(), '-name', extension]
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            output = proc.stdout.read().decode('utf-8').split()
        except Exception:
            output = []
        return output

    print('Getting list of assets...\n')
    filelist = [_file_dir_files(directory) for directory in dirs]
    # watch progress
    with ProgressBar():
        filelist = dask.compute(*filelist)

    filelist = list(itertools.chain(*filelist))
    print('\nDone...\n')
    return filelist
