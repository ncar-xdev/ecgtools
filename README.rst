.. image:: https://img.shields.io/github/workflow/status/NCAR/ecg/CI?logo=github&style=for-the-badge
    :target: https://github.com/NCAR/ecg/actions
    :alt: GitHub Workflow CI Status

.. image:: https://img.shields.io/github/workflow/status/NCAR/ecg/code-style?label=Code%20Style&style=for-the-badge
    :target: https://github.com/NCAR/ecg/actions
    :alt: GitHub Workflow Code Style Status

.. image:: https://img.shields.io/codecov/c/github/NCAR/ecg.svg?style=for-the-badge
    :target: https://codecov.io/gh/NCAR/ecg

ecg
===

Development
------------

For a development install, do the following in the repository directory:

.. code-block:: bash

    conda env update -f ci/environment.yml
    conda activate sandbox-devel
    python -m pip install -e .
