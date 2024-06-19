import requests
from pathlib import Path
from math import log, floor
from urllib.parse import urlparse
from typing import Union, Optional
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn


def format_size(size):
    factor = 1024
    units = ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB')
    idx = 0
    if size > 0:
        p = log(size, factor)
        idx = floor(p) if p < len(units) else len(units) - 1
        size = (factor ** (p - idx))
    return f"{size:.2f} {units[idx]}"


class SizeColumn(TextColumn):
    def __init__(self):
        super().__init__(
            text_format="{task.completed}/{task.total}", justify="left"
        )

    def render(self, task):
        total = "[orange]???" if task.total is None else format_size(task.total)
        if task.finished:
            return f"{format_size(task.completed)} [green]✔"
        return f"{format_size(task.completed)}/{total}"


def download(
        url: str,
        dst_path: Optional[Union[str, Path]] = None,
        chunk_size: Optional[int] = 8192
):
    file_name = Path(urlparse(url).path).name
    if dst_path is None:
        dst_path = Path("~/Downloads").expanduser().absolute() / file_name
    else:
        dst_path = Path(dst_path).absolute()
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', None))

        with open(dst_path, 'wb') as file:
            if total_size:
                with Progress(
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TextColumn("{task.percentage:>3.1f}%"),
                        TextColumn(""),
                        SizeColumn()
                ) as progress:
                    task = progress.add_task(description=f"Downloading {file_name} :", total=total_size)
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        file.write(chunk)
                        progress.update(task, advance=len(chunk))
            else:
                if isinstance(total_size, int) and total_size == 0:
                    raise ValueError("Corrupt or empty file.!!!")
                with Progress(
                        SpinnerColumn(),
                        TextColumn(f"[bold blue]Downloading {file_name} "),
                        SizeColumn()
                ) as progress:
                    task = progress.add_task(description="Downloading", total=None)
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        file.write(chunk)
                        progress.update(task, advance=1)
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Error downloading {file_name}: {exc}")
