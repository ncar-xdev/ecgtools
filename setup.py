#!/usr/bin/env python3

"""The setup script."""

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().strip().split('\n')

with open('README.md') as f:
    long_description = f.read()

setup(
    maintainer='NCAR XDev Team',
    maintainer_email='xdev@ucar.edu',
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Scientific/Engineering',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
    ],
    description='ESM Catalog Generation Utilities',
    install_requires=requirements,
    license='Apache Software License 2.0',
    long_description=long_description,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='ecgtools',
    name='ecgtools',
    packages=find_packages(include=['ecgtools', 'ecgtools.*']),
    entry_points={},
    url='https://github.com/ncar-xdev/ecgtools',
    project_urls={
        'Documentation': 'https://ecgtools.readthedocs.io/',
        'Source': 'https://github.com/ncar-xdev/ecgtools',
        'Tracker': 'https://github.com/ncar-xdev/ecgtools/issues',
    },
    use_scm_version={
        'version_scheme': 'post-release',
        'local_scheme': 'dirty-tag',
    },
    zip_safe=False,
)
