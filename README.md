# ecgtools

| CI          | [![GitHub Workflow Status][github-ci-badge]][github-ci-link] [![Code Coverage Status][codecov-badge]][codecov-link] [![pre-commit.ci status][pre-commit.ci-badge]][pre-commit.ci-link] |
| :---------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| **Docs**    |                                                                     [![Documentation Status][rtd-badge]][rtd-link]                                                                     |
| **Package** |                                                          [![Conda][conda-badge]][conda-link] [![PyPI][pypi-badge]][pypi-link]                                                          |
| **License** |                                                                         [![License][license-badge]][repo-link]                                                                         |

## Motivation

The critical requirement for using [`intake-esm`](https://github.com/intake/intake-esm) is having a data catalog. This package enables you to build data catalogs to be read in by [`intake-esm`](https://github.com/intake/intake-esm), which enables a user to easily search, discover, and access datasets they are interested in using.

See [documentation](https://ecgtools.readthedocs.io) for more information.

## Installation

ecgtools can be installed from PyPI with pip:

```bash
python -m pip install ecgtools
```

It is also available from conda-forge for conda installations:

```bash
conda install -c conda-forge ecgtools
```

[github-ci-badge]: https://github.com/ncar-xdev/ecgtools/actions/workflows/ci.yaml/badge.svg
[github-ci-link]: https://github.com/ncar-xdev/ecgtools/actions/workflows/ci.yaml
[codecov-badge]: https://img.shields.io/codecov/c/github/ncar-xdev/ecgtools.svg?logo=codecov
[codecov-link]: https://codecov.io/gh/ncar-xdev/ecgtools
[rtd-badge]: https://img.shields.io/readthedocs/ecgtools/latest.svg
[rtd-link]: https://ecgtools.readthedocs.io/en/latest/?badge=latest
[pypi-badge]: https://img.shields.io/pypi/v/ecgtools?logo=pypi
[pypi-link]: https://pypi.org/project/ecgtools
[conda-badge]: https://img.shields.io/conda/vn/conda-forge/ecgtools?logo=anaconda
[conda-link]: https://anaconda.org/conda-forge/ecgtools
[license-badge]: https://img.shields.io/github/license/ncar-xdev/ecgtools
[repo-link]: https://github.com/ncar-xdev/ecgtools
[pre-commit.ci-badge]: https://results.pre-commit.ci/badge/github/ncar-xdev/ecgtools/main.svg
[pre-commit.ci-link]: https://results.pre-commit.ci/latest/github/ncar-xdev/ecgtools/main
