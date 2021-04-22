import pathlib
from enum import Enum
from typing import List

import typer
from distributed import Client
from ncar_jobqueue import NCARCluster

from .builders.cesm import smyle_parser
from .builders.cmip import cmip6_parser
from .core import Builder, console

app = typer.Typer(help='ESM Catalog Generation CLI')

parsers = {'cesm2-smyle': smyle_parser, 'cmip6': cmip6_parser}


class Collection(Enum):
    cesm2_smyle = 'cesm2-smyle'
    cmip6 = 'cmip6'


class DataFormat(Enum):
    netcdf = 'netcdf'
    zarr = 'zarr'


@app.command()
def build(
    collection: Collection = typer.Argument(..., help='Collection name', case_sensitive=False),
    root_path: str = typer.Argument(..., help='Path of root directory'),
    extension: str = typer.Option('*.nc', help='File extension', show_default=True),
    depth: int = typer.Option(
        3,
        help='Recursion depth. Recursively crawl `root_path` up to a specified depth',
        show_default=True,
    ),
    exclude_patterns: List[str] = typer.Option(
        None,
        help='Directory, file patterns to exclude during catalog generation',
        show_default=True,
    ),
    nbatches: int = typer.Option(
        25, help='Number of tasks to batch in a single `dask.delayed` call'
    ),
    jobs: int = typer.Option(4, help='Number of batch jobs to submit'),
    path_column: str = typer.Option(
        'path', help='The name of the column containing the path to the asset.'
    ),
    variable_column: str = typer.Option(
        'variable', help='Name of the attribute column in catalog that contains the variable name'
    ),
    data_format: DataFormat = typer.Option('netcdf', help='Data format'),
    catalog_name: str = typer.Option(None, help='Catalog Name'),
    description: str = typer.Option(
        '', help='Detailed description to fully explain the collection'
    ),
    output_path: pathlib.Path = typer.Option(
        '.', help='Output directory path', exists=True, dir_okay=True
    ),
):
    """Generates a catalog from a list of files."""

    cluster = NCARCluster(
        cores=12, processes=12, memory='120GB', resource_spec='select=1:ncpus=12:mem=120GB'
    )
    cluster.adapt(minimum_jobs=jobs, maximum_jobs=jobs)
    client = Client(cluster)

    with console.status(f'[bold green]Building catalog for {collection.value}...'):
        if catalog_name is None:
            catalog_file = f'{collection.value}.csv'
        else:
            catalog_file = f'{catalog_name}.csv'
        catalog_file = output_path / catalog_file
        _ = (
            Builder(
                root_path,
                extension=extension,
                depth=depth,
                parser=parsers[collection.value],
                exclude_patterns=exclude_patterns,
                nbatches=nbatches,
            )
            .build(
                path_column=path_column,
                variable_column=variable_column,
                data_format=data_format.value,
                description=description,
            )
            .save(catalog_file=catalog_file)
        )
    cluster.close()
    client.close()


@app.command()
def versions():
    """print the versions of ecgtools and its dependencies."""
    import importlib
    import sys

    file = sys.stdout

    packages = [
        'xarray',
        'pandas',
        'intake',
        'ncar_jobqueue',
        'dask_jobqueue',
        'cf_xarray',
        'dask',
        'netCDF4',
        'zarr',
        'typer',
    ]
    deps = [(mod, lambda mod: mod.__version__) for mod in packages]

    deps_blob = []
    for (modname, ver_f) in deps:
        try:
            if modname in sys.modules:
                mod = sys.modules[modname]
            else:
                mod = importlib.import_module(modname)
        except Exception:
            deps_blob.append((modname, None))
        else:
            try:
                ver = ver_f(mod)
                deps_blob.append((modname, ver))
            except Exception:
                deps_blob.append((modname, 'installed'))

    print('\nINSTALLED VERSIONS', file=file)
    print('------------------', file=file)

    print('', file=file)
    for k, stat in sorted(deps_blob):
        print(f'{k}: {stat}', file=file)


def main():
    typer.run(app())
