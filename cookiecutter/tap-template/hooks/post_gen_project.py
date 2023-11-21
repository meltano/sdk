#!/usr/bin/env python
from pathlib import Path


BASE_PATH = Path('{{cookiecutter.library_name}}')


def delete_folder(pth: Path):
    for sub in pth.iterdir():
        if sub.is_dir():
            delete_folder(sub)
        else:
            sub.unlink()
    pth.rmdir()


if __name__ == '__main__':

    # Rename stream type client and delete others
    target = Path(BASE_PATH, 'client.py')
    Path(BASE_PATH, '{{cookiecutter.stream_type|lower}}-client.py').rename(target)
    [c.unlink() for c in Path(BASE_PATH).rglob("*-client.py")]

    if '{{ cookiecutter.auth_method }}' not in ('OAuth2', 'JWT'):
        Path(BASE_PATH, 'auth.py').unlink()

    if '{{ cookiecutter.stream_type }}' == 'SQL':
        Path(BASE_PATH, 'streams.py').unlink()

    if '{{ cookiecutter.license }}' != 'Apache-2.0':
        Path('LICENSE').unlink()

    if '{{ cookiecutter.include_ci_files }}' != 'GitHub':
        delete_folder(Path('.github'))

