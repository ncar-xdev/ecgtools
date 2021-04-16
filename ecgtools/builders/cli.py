from typing import List

import typer
from distributed import Client
from ncar_jobqueue import NCARCluster
from rich.console import Console

console = Console()
from ..core import Builder
from .cesm import symle_parser

app = typer.Typer(help='ESM Catalog Generation CLI')


@app.command()
def symle(
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
):

    cluster = NCARCluster(cores=8, processes=8, memory='80GB')
    cluster.scale(jobs=jobs)
    client = Client(cluster)

    with console.status('[bold green]Building catalog...'):
        _ = (
            Builder(
                root_path,
                extension=extension,
                depth=depth,
                parser=symle_parser,
                exclude_patterns=exclude_patterns,
                nbatches=nbatches,
            )
            .build(path_column='path', variable_column='variable')
            .save(catalog_file='campaign-cesm2-symle.csv')
        )
    cluster.close()
    client.close()


@app.command()
def test():
    ...


def main():
    typer.run(app())


#  root_path: str,
#     extension: str = '*.nc',
#     depth: int = 0,
#     exclude_patterns: list = None,
#     parser: <built-in function callable> = None,
#     lazy: bool = True,
#     nbatches: int = 25,
