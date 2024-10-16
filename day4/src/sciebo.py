from pathlib import Path
import requests
from typing import Union


def download_file(public_url, to_filename):
    """Downloads a file from a shared URL on Sciebo."""
    _make_parent_folders(to_filename)

    r = requests.get(public_url + "/download", stream=True)
    if not 'Content-Length' in r.headers:
        raise IOError(f"Data not found at {public_url}")
        
    num_bytes = int(r.headers['Content-Length'])
    progress_bar = _ProgressBar(desc=f"Downloading {to_filename}", unit='B', unit_scale=True, total=num_bytes)
    with open(to_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
            progress_bar.update(len(chunk))


def download_folder(public_url, to_filename):
    """Downloads a folder from a shared URL on Sciebo."""
    _make_parent_folders(to_filename)
    
    r = requests.get(public_url + "/download", stream=True)
    progress_bar = _ProgressBar()
    with open(to_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
            progress_bar.update(len(chunk))


def download_from_sciebo(public_url, to_filename, is_file = True):
    """Wrapper function: Downloads a file or folder from a shared URL on Sciebo."""
    download_fun = download_file if is_file else download_folder
    download_fun(public_url=public_url, to_filename=to_filename)


def _make_parent_folders(path: Union[Path, str]) -> None:
    # Create the parent folders if a longer path was described
    path = Path(path)
    if len(path.parts) > 1:
        Path(path).parent.mkdir(parents=True, exist_ok=True)


class _ProgressBar:
    def __init__(self, desc=None, unit=None, unit_scale=True, total=None) -> None:
        """Creates tqdm progress bar if tqdm is installed, else acts as a Null operator."""
        self.pbar: Optional['tqdm']
        try:
            from tqdm import tqdm
            self.pbar = tqdm(desc=desc, unit=unit, unit_scale=unit_scale, total=total)
        except (ModuleNotFoundError, ImportError):
            self.pbar = None

    def update(self, nbytes: int) -> None:
        if self.pbar:
            self.pbar.update(nbytes)