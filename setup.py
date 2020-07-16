#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().strip().split('\n')

with open('README.rst') as f:
    long_description = f.read()

setup(
    maintainer='NCAR XDev Team',
    maintainer_email='xdev@ucar.edu',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Scientific/Engineering',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
    ],
    description='ESM Catalog Generation Utilities',
    install_requires=requirements,
    license='Apache Software License 2.0',
    long_description=long_description,
    include_package_data=True,
    keywords='ecgtools',
    name='ecgtools',
    packages=find_packages(include=['ecgtools', 'ecgtools.*']),
    data_files=[('schema', ['schema/generic_schema.yaml'])],
    url='https://github.com/NCAR/ecgtools',
    project_urls={
        'Documentation': 'https://github.com/NCAR/ecgtools',
        'Source': 'https://github.com/NCAR/ecgtools',
        'Tracker': 'https://github.com/NCAR/ecgtools/issues',
    },
    zip_safe=False,
)
