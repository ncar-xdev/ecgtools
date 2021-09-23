# ecgtools

- [ecgtools](#ecgtools)
  - [Badges](#badges)

## Badges

| CI          | [![GitHub Workflow Status][github-ci-badge]][github-ci-link] [![GitHub Workflow Status][github-lint-badge]][github-lint-link] [![Code Coverage Status][codecov-badge]][codecov-link] |
| :---------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| **Docs**    |                                                                    [![Documentation Status][rtd-badge]][rtd-link]                                                                    |
| **Package** |                                                         [![Conda][conda-badge]][conda-link] [![PyPI][pypi-badge]][pypi-link]                                                         |
| **License** |                                                                        [![License][license-badge]][repo-link]                                                                        |

<!-- ## Installation (COMING SOON)

ecgtools can be installed from PyPI with pip:

```bash
python -m pip install ecgtools
```

ecgtools is also available from conda-forge for conda installations:

```bash
conda install -c conda-forge ecgtools
```

See [documentation](https://ecgtools.readthedocs.io) for more information. -->

[github-ci-badge]: https://img.shields.io/github/workflow/status/NCAR/ecgtools/CI?label=CI&logo=github&style=for-the-badge
[github-lint-badge]: https://img.shields.io/github/workflow/status/NCAR/ecgtools/linting?label=linting&logo=github&style=for-the-badge
[github-ci-link]: https://github.com/NCAR/ecgtools/actions?query=workflow%3ACI
[github-lint-link]: https://github.com/NCAR/ecgtools/actions?query=workflow%3Alinting
[codecov-badge]: https://img.shields.io/codecov/c/github/NCAR/ecgtools.svg?logo=codecov&style=for-the-badge
[codecov-link]: https://codecov.io/gh/NCAR/ecgtools
[rtd-badge]: https://img.shields.io/readthedocs/ecgtools/latest.svg?style=for-the-badge
[rtd-link]: https://ecgtools.readthedocs.io/en/latest/?badge=latest
[pypi-badge]: https://img.shields.io/pypi/v/ecgtools?logo=pypi&style=for-the-badge
[pypi-link]: https://pypi.org/project/ecgtools
[conda-badge]: https://img.shields.io/conda/vn/conda-forge/ecgtools?logo=anaconda&style=for-the-badge
[conda-link]: https://anaconda.org/conda-forge/ecgtools
[license-badge]: https://img.shields.io/github/license/NCAR/ecgtools?style=for-the-badge
[repo-link]: https://github.com/NCAR/ecgtools

## Motivation

Similar to [`intake-esm`](https://github.com/intake/intake-esm), this package helps with finding, investigating, and loading data. The critical requirement for using [`intake-esm`](https://github.com/intake/intake-esm) is having a data catalog. This package enables you build data catalogs to be read in by [`intake-esm`](https://github.com/intake/intake-esm), which enables a user to easily search, discover, and access datasets they are interested in using.

## Installation

ecgtools can be installed from PyPI with pip:

```bash
python -m pip install ecgtools
```

It is also available from conda-forge for conda installations:

```bash
conda install -c conda-forge ecgtools
```
