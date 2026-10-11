"""Release regressions: fail closed on registry errors and resume partial uploads."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

spec = importlib.util.spec_from_file_location('release_publish', Path(__file__).with_name('publish.py'))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class PublishTests(unittest.TestCase):
    def test_only_registry_404_means_missing(self):
        for code in [403, 429, 500]:
            with patch.object(release, 'urlopen', side_effect=HTTPError('url', code, 'error', {}, None)):
                with self.assertRaises(HTTPError):
                    release.get_json('https://crates.io/api/v1/crates/vibe-coding-analytics')
        with patch.object(release, 'urlopen', side_effect=HTTPError('url', 404, 'missing', {}, None)):
            self.assertIsNone(release.get_json('https://crates.io/api/v1/crates/vibe-coding-analytics'))

    def test_conflicting_crate_ownership_blocks_publication(self):
        with patch.object(release, 'get_json', return_value={'crate': {'repository': 'https://example.com/other'}}):
            with self.assertRaisesRegex(RuntimeError, 'different repository'):
                release.is_published('vibe-coding-analytics', '0.3.0')

    def test_version_must_be_confirmed_by_registry(self):
        crate = {'crate': {'repository': release.REPOSITORY}}
        with patch.object(release, 'get_json', side_effect=[crate, {'version': {'num': '0.3.0'}}]):
            self.assertTrue(release.is_published('vibe-coding-analytics', '0.3.0'))
        with patch.object(release, 'get_json', side_effect=[crate, {'errors': []}]):
            with self.assertRaisesRegex(RuntimeError, 'Unexpected registry response'):
                release.is_published('vibe-coding-analytics', '0.3.0')

    def test_tag_versions_and_dependency_must_match(self):
        packages = [
            {'name': 'vibe-coding-analytics', 'version': '0.3.0'},
            {'name': 'paceflow', 'version': '0.3.0', 'dependencies': [{'name': 'vibe-coding-analytics', 'req': '=0.3.0'}]},
        ]
        with patch.object(release.subprocess, 'check_output', return_value=json.dumps({'packages': packages}).encode()):
            with patch.dict(release.os.environ, {'GITHUB_REF_NAME': 'v0.3.0'}):
                self.assertEqual(release.versions(), '0.3.0')
            with patch.dict(release.os.environ, {'GITHUB_REF_NAME': 'v0.2.5'}):
                with self.assertRaisesRegex(RuntimeError, 'must match'):
                    release.versions()
            packages[1]['dependencies'][0]['req'] = '0.3'
        with patch.object(release.subprocess, 'check_output', return_value=json.dumps({'packages': packages}).encode()):
            with self.assertRaisesRegex(RuntimeError, 'exact matching'):
                release.versions()

    def test_partial_release_skips_vca_and_verifies_paceflow_before_upload(self):
        with patch.object(release, 'versions', return_value='0.3.0'), \
             patch.object(release, 'is_published', side_effect=[True, False]), \
             patch.object(release, 'wait_for_vca') as wait, \
             patch.object(release.subprocess, 'run') as run, \
             patch.dict(release.os.environ, {'CARGO_REGISTRY_TOKEN': 'test-only'}), \
             patch('sys.argv', ['publish.py']):
            release.main()
        wait.assert_called_once_with('0.3.0')
        commands = [call.args[0] for call in run.call_args_list]
        publishes = [c for c in commands if c[:2] == ['cargo', 'publish']]
        self.assertEqual(publishes, [
            ['cargo', 'publish', '-p', 'paceflow', '--locked', '--dry-run'],
            ['cargo', 'publish', '-p', 'paceflow', '--locked'],
        ])


if __name__ == '__main__':
    unittest.main()
