import pathlib
from enum import Enum
from typing import List

import typer
from distributed import Client
from ncar_jobqueue import NCARCluster
from rich.console import Console

console = Console()
from ..core import Builder
from .cesm import symle_parser

app = typer.Typer(help='ESM Catalog Generation CLI')

parsers = {'cesm2_symle': symle_parser}


class Collection(Enum):
    cesm2_symle = 'cesm2-symle'


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
    data_format: DataFormat = typer.Option('netcdf', help='The data format. '),
    catalog_name: str = typer.Option(None, help='Catalog Name'),
    output_path: pathlib.Path = typer.Option(
        '.', help='Output directory path', exists=True, dir_okay=True
    ),
):

    cluster = NCARCluster(cores=8, processes=8, memory='80GB')
    cluster.scale(jobs=jobs)
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
                path_column=path_column, variable_column=variable_column, data_format=data_format
            )
            .save(catalog_file=catalog_file)
        )
    cluster.close()
    client.close()


@app.command()
def test():
    ...


def main():
    typer.run(app())
