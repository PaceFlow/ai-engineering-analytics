#!/usr/bin/env python3
"""Check a release archive's layout, checksum, command identity and version."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import tarfile
import tempfile
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('product', choices=['vca', 'paceflow'])
    parser.add_argument('target')
    parser.add_argument('version')
    args = parser.parse_args()
    base = f'{args.product}-{args.target}'
    windows = 'windows' in args.target
    archive = Path('dist') / (base + ('.zip' if windows else '.tar.gz'))
    checksum = Path(str(archive) + '.sha256').read_text().split()[0]
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == checksum
    with tempfile.TemporaryDirectory(prefix='analytics-binary-archive-') as directory:
        root = Path(directory)
        if windows:
            with zipfile.ZipFile(archive) as contents:
                names = [n.replace('\\', '/') for n in contents.namelist()]
                assert all(n.startswith(base + '/') for n in names), names
                contents.extractall(root)
        else:
            with tarfile.open(archive) as contents:
                names = contents.getnames()
                assert all(n == base or n.startswith(base + '/') for n in names), names
                contents.extractall(root)
        binary = root / base / (args.product + ('.exe' if windows else ''))
        version = subprocess.check_output([str(binary), '--version'], text=True)
        assert version.startswith(f'{args.product} {args.version} '), version
        help_text = subprocess.check_output([str(binary), '--help'], text=True)
        assert f'Usage: {args.product}' in help_text
        assert (root / base / 'README.md').is_file()
        assert (root / base / 'LICENSE').is_file()
        if args.product == 'vca':
            assert 'paceflow' not in help_text.lower()
    print(f'Verified {archive} layout, checksum and command.')


if __name__ == '__main__':
    main()
