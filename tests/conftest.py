import os
import pathlib

import pytest


@pytest.fixture
def sample_data_directory():
    return pathlib.Path(os.path.dirname(__file__)).parent / 'sample_data'
