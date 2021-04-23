import pathlib
import socket
from enum import Enum
from typing import List

import typer
from distributed import Client
from ncar_jobqueue import NCARCluster

from .builders.cesm import smyle_parser
from .builders.cmip import cmip6_parser
from .core import Builder, console

app = typer.Typer(help='ESM Catalog Generation CLI')

parsers = {'cesm2_smyle': smyle_parser, 'cmip6': cmip6_parser}


def create_cluster(jobs):
    cluster = NCARCluster(
        cores=12, processes=12, memory='120GB', resource_spec='select=1:ncpus=12:mem=120GB'
    )
    cluster.adapt(minimum_jobs=jobs, maximum_jobs=jobs)
    client = Client(cluster)
    host = client.run_on_scheduler(socket.gethostname)
    port = client.scheduler_info()['services']['dashboard']
    console.print(
        '[bold cyan]To access the dashboard link',
        '[bold cyan]run the following (make sure to replace LOGIN_NODE_ADDRESS with the appropriate value)',
        f'[bold cyan]\nssh -N -L {port}:{host}:{port} LOGIN_NODE_ADDRESS',
    )
    return client, cluster


def _build(
    depth=None,
    extension=None,
    exclude_patterns=None,
    nbatches=None,
    path_column=None,
    variable_column=None,
    data_format=None,
    description=None,
    catalog_name=None,
    collection=None,
    root_path=None,
    output_path=None,
    parser=None,
    format_column=None,
    attributes=None,
    groupby_attrs=None,
    aggregations=None,
):

    with console.status(f'[bold green]Building catalog for {collection}...'):
        if catalog_name is None:
            catalog_file = f'{collection}.csv'
        else:
            catalog_file = f'{catalog_name}.csv'
        catalog_file = output_path / catalog_file
        _ = (
            Builder(
                root_path,
                extension=extension,
                depth=depth,
                parser=parser,
                exclude_patterns=exclude_patterns,
                nbatches=nbatches,
            )
            .build(
                path_column=path_column,
                variable_column=variable_column,
                data_format=data_format,
                description=description,
                attributes=attributes,
                format_column=format_column,
                aggregations=aggregations,
                groupby_attrs=groupby_attrs,
                cat_id=catalog_name,
            )
            .save(catalog_file=catalog_file)
        )


class Collection(Enum):
    cesm2_smyle = 'cesm2_smyle'
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
    format_column: str = typer.Option(
        ...,
        help='Column name which contains the data format, allowing for multiple data assets (file formats) types in one catalog. Mutually exclusive with `data_format`.',
    ),
    attributes: List[str] = typer.Option(
        ...,
        help=' A list of attributes. An attribute dictionary describes a column in the catalog CSV file.',
    ),
    groupby_attrs: List[str] = typer.Option(
        ..., help='Column names (attributes) that define data sets that can be aggegrated'
    ),
    aggregations: List[str] = typer.Option(
        ..., help='List of aggregations to apply to query results'
    ),
    catalog_name: str = typer.Option(None, help='Catalog Name'),
    description: str = typer.Option(
        '', help='Detailed description to fully explain the collection'
    ),
    output_path: pathlib.Path = typer.Option(
        '.', help='Output directory path', exists=True, dir_okay=True
    ),
):
    """Generates a catalog from a list of files."""

    client, cluster = create_cluster(jobs)
    parser = parsers[collection.value]
    _build(
        depth=depth,
        extension=extension,
        exclude_patterns=exclude_patterns,
        nbatches=nbatches,
        path_column=path_column,
        variable_column=variable_column,
        data_format=data_format.value,
        description=description,
        catalog_name=catalog_name,
        collection=collection.value,
        root_path=root_path,
        output_path=output_path,
        parser=parser,
        format_column=format_column,
        attributes=attributes,
        groupby_attrs=groupby_attrs,
        aggregations=aggregations,
    )
    cluster.close()
    client.close()


@app.command()
def build_from_config(
    config: pathlib.Path = typer.Argument(..., help='YAML config file', file_okay=True)
):
    """Generates a catalog from a list of files (uses a YAML config file)."""
    import yaml

    with config.open() as f:
        data = yaml.safe_load(f)

    jobs = data.get('jobs', 10)
    depth = data.get('depth', 4)
    extension = data.get('extension', '*.nc')
    exclude_patterns = data.get('exclude_patterns', [])
    nbatches = data.get('nbatches', 25)
    path_column = data['path_column']
    variable_column = data['variable_column']
    data_format = data.get('data_format')
    format_column = data.get('format_column')
    groupby_attrs = data.get('groupby_attrs', [])
    aggregations = data.get('aggregations', [{}])
    description = data.get('description', '')
    attributes = data.get('attributes')
    catalog_name = data.get('catalog_name')
    collection = data['collection']
    root_path = pathlib.Path(data['root_path'])
    parser = parsers[collection]
    output_path = pathlib.Path(data.get('output_path', '.'))
    client, cluster = create_cluster(jobs)
    _build(
        depth=depth,
        extension=extension,
        exclude_patterns=exclude_patterns,
        nbatches=nbatches,
        path_column=path_column,
        variable_column=variable_column,
        data_format=data_format,
        description=description,
        catalog_name=catalog_name,
        collection=collection,
        root_path=root_path,
        output_path=output_path,
        parser=parser,
        format_column=format_column,
        attributes=attributes,
        groupby_attrs=groupby_attrs,
        aggregations=aggregations,
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
