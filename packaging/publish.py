#!/usr/bin/env python3
"""Publish a tested matching release, VCA first, with resumable registry checks."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = 'https://github.com/PaceFlow/ai-engineering-analytics'


def get_json(url):
    request = Request(url, headers={'User-Agent': 'analytics-release (github.com/PaceFlow/ai-engineering-analytics)'})
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as error:
        if error.code == 404:
            return None
        raise


def versions():
    metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--no-deps', '--format-version', '1'], cwd=ROOT))
    packages = {p['name']: p for p in metadata['packages'] if p['name'] in ['vibe-coding-analytics', 'paceflow']}
    values = {p['version'] for p in packages.values()}
    if set(packages) != {'vibe-coding-analytics', 'paceflow'} or len(values) != 1:
        raise RuntimeError('VCA and Paceflow must declare the same release version')
    version = values.pop()
    dependency = next(d for d in packages['paceflow']['dependencies'] if d['name'] == 'vibe-coding-analytics')
    if dependency['req'] != '=' + version:
        raise RuntimeError('Paceflow must depend on the exact matching VCA release')
    tag = os.environ.get('GITHUB_REF_NAME')
    if tag != 'v' + version:
        raise RuntimeError(f'Release tag {tag!r} must match v{version}')
    return version


def is_published(name, version):
    crate = get_json(f'https://crates.io/api/v1/crates/{name}')
    if crate is None:
        return False
    if (crate['crate'].get('repository') or '').rstrip('/') != REPOSITORY:
        raise RuntimeError(f'The {name} registry name belongs to a different repository')
    result = get_json(f'https://crates.io/api/v1/crates/{name}/{version}')
    if result is None:
        return False
    if result.get('version', {}).get('num') != version:
        raise RuntimeError(f'Unexpected registry response for {name} {version}')
    return True


def wait_for_vca(version):
    # Check the sparse index as well as the API: Cargo resolves dependencies here.
    for _ in range(60):
        request = Request('https://index.crates.io/vi/be/vibe-coding-analytics', headers={'User-Agent': 'analytics-release'})
        try:
            with urlopen(request, timeout=30) as response:
                releases = [json.loads(line) for line in response.read().decode().splitlines()]
            if any(r['vers'] == version and not r.get('yanked') for r in releases):
                return
        except HTTPError as error:
            if error.code != 404:
                raise
        time.sleep(5)
    raise RuntimeError(f'VCA {version} was not available in the registry index within five minutes; rerun the release job')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-tag', action='store_true')
    args = parser.parse_args()
    version = versions()
    if args.check_tag:
        print(f'Both packages and the VCA dependency match v{version}.')
        return
    if not os.environ.get('CARGO_REGISTRY_TOKEN'):
        raise RuntimeError('CRATES_PUBLISH_TOKEN must permit creating VCA and updating both crates')
    for name in ['vibe-coding-analytics', 'paceflow']:
        if name == 'paceflow':
            wait_for_vca(version)
        if is_published(name, version):
            print(f'{name} {version} is already published; skipping upload.')
            continue
        subprocess.run(['cargo', 'publish', '-p', name, '--locked', '--dry-run'], cwd=ROOT, check=True)
        subprocess.run(['cargo', 'publish', '-p', name, '--locked'], cwd=ROOT, check=True)
        print(f'Published {name} {version}.')
    # A separate smoke install exercises real registry resolution, not local paths.
    import tempfile
    with tempfile.TemporaryDirectory(prefix='analytics-registry-install-') as directory:
        for name in ['vibe-coding-analytics', 'paceflow']:
            subprocess.run(['cargo', 'install', '--locked', '--version', version, '--root', directory, name], cwd=ROOT, check=True)
            binary = 'vca' if name == 'vibe-coding-analytics' else name
            subprocess.run([str(Path(directory) / 'bin' / binary), '--version'], check=True)


if __name__ == '__main__':
    main()
