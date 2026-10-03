#!/usr/bin/env python3
"""Verify the exact source archives without relying on the checkout or Git."""
import argparse
import json
import os
import re
from pathlib import Path
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def run(*args, cwd=ROOT):
    subprocess.run(args, cwd=cwd, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--allow-dirty', action='store_true', help='Package local uncommitted implementation changes')
    args = parser.parse_args()
    metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--no-deps', '--format-version', '1'], cwd=ROOT))
    packages = {p['name']: p for p in metadata['packages'] if p['name'] in ['vibe-coding-analytics', 'paceflow']}
    assert set(packages) == {'vibe-coding-analytics', 'paceflow'}
    command = ['cargo', 'package', '--workspace', '--locked', '--no-verify']
    if args.allow_dirty:
        command.append('--allow-dirty')
    run(*command)
    with tempfile.TemporaryDirectory(prefix='analytics-source-packages-') as directory:
        stage = Path(directory)
        members = []
        for name in ['vibe-coding-analytics', 'paceflow']:
            version = packages[name]['version']
            member = f'{name}-{version}'
            archive = Path(metadata['target_directory']) / 'package' / f'{member}.crate'
            with tarfile.open(archive) as contents:
                names = contents.getnames()
                forbidden = ['/.git/', '/tests/', '/target/', '/examples/']
                assert not any(any(part in n for part in forbidden) or n.endswith('.db') for n in names), names
                assert all(not Path(n).is_absolute() and '..' not in Path(n).parts for n in names)
                contents.extractall(stage)
            members.append(member)
        # A local registry patch is needed only before the first VCA publication.
        # Everything referenced by it comes from the packaged VCA archive.
        (stage / 'Cargo.toml').write_text(
            '[workspace]\nresolver = "3"\nmembers = ' + json.dumps(members) + '\n'
            '[patch.crates-io]\nvibe-coding-analytics = { path = ' + json.dumps(members[0]) + ' }\n'
        )
        # Preserve the versions actually shipped in Cargo.lock. Only replace
        # VCA's registry identity with the extracted local package for this check.
        lock = (stage / members[1] / 'Cargo.lock').read_text()
        vca_lock = (stage / members[0] / 'Cargo.lock').read_text()
        package_pattern = r'\[\[package\]\]\nname = "vibe-coding-analytics"\n.*?(?=\n\[\[package\]\]|\Z)'
        local_vca = re.search(package_pattern, vca_lock, re.S).group(0)
        lock = re.sub(package_pattern, lambda _: local_vca, lock, flags=re.S)
        (stage / 'Cargo.lock').write_text(lock)
        run('cargo', 'build', '--workspace', '--locked', cwd=stage)
        install_root = stage / 'installed'
        for name, member in zip(['vibe-coding-analytics', 'paceflow'], members):
            run('cargo', 'install', '--path', member, '--locked', '--debug', '--root', str(install_root), cwd=stage)
            command = 'vca' if name == 'vibe-coding-analytics' else name
            binary = install_root / 'bin' / (command + ('.exe' if os.name == 'nt' else ''))
            help_text = subprocess.check_output([str(binary), '--help'], text=True)
            assert f'Usage: {command}' in help_text
            if command == 'vca':
                assert 'paceflow' not in help_text.lower()
                assert '  sync ' not in help_text and '  hooks ' not in help_text
        actual = {p.stem for p in (install_root / 'bin').iterdir()}
        assert actual == {'vca', 'paceflow'}, actual
        print('Verified both extracted source archives and side-by-side installation.')


if __name__ == '__main__':
    main()
