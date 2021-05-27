import re


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
