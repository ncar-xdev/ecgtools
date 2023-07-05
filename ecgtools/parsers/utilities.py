import re

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
