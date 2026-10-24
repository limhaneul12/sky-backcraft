"""Regression coverage for manifest-driven, verified package reception."""
import copy
import gzip
import hashlib
import json
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch

receiver = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'download-export.py'), run_name='receiver')


def fixture(version='spot-lab-export-v4'):
    run = 'run-fixture'
    data = {'artifact-review': b'{"review":true}', 'artifact-ledger': gzip.compress(b'{"ledger":true}', mtime=0)}
    descriptors = []
    refs = []
    for artifact_id, name in [('artifact-review', 'review.json'), ('artifact-ledger', 'ledger.json.gz')]:
        raw = data[artifact_id]
        decoded = gzip.decompress(raw) if name.endswith('.gz') else None
        desc = {'artifact_id': artifact_id, 'run_id': run, 'file_name': name,
                'media_type': 'application/gzip' if decoded else 'application/json',
                'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'complete': True,
                'uncompressed_bytes': len(decoded) if decoded else None,
                'uncompressed_sha256': hashlib.sha256(decoded).hexdigest() if decoded else None,
                'retrieval': {'tool_name': 'artifact_query', 'read_arguments': {'action': 'read'}}}
        descriptors.append(desc)
        refs.append({'id': artifact_id if version.endswith('v4') else 'package-local-' + artifact_id,
                     'run_id': run, 'relative_path': name, **{k: desc[k] for k in (
                         'media_type', 'bytes', 'sha256', 'complete', 'uncompressed_bytes', 'uncompressed_sha256')}})
    manifest = {'schema_version': version, 'run_id': run, 'scope': {'kind': 'FULL'}, 'artifacts': refs}
    raw = json.dumps(manifest).encode()
    data['artifact-manifest'] = raw
    descriptors.append({'artifact_id': 'artifact-manifest', 'run_id': run, 'file_name': 'manifest.json',
                        'media_type': 'application/json', 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
                        'complete': True, 'uncompressed_bytes': None, 'uncompressed_sha256': None,
                        'retrieval': {'tool_name': 'artifact_query', 'read_arguments': {'action': 'read'}}})
    return {'job': {'payload': {'kind': 'EXPORT', 'run_id': run, 'market': None}}, 'artifacts': descriptors}, data


class Client:
    def __init__(self, job, data, fail_id=None):
        self.artifacts = {a['artifact_id']: a for a in job['artifacts']}
        self.data = data
        self.fail_id = fail_id
        self.read_ids = []

    def tool(self, name, args):
        assert name == 'artifact_query' and args['action'] == 'read'
        artifact_id = args['artifact_id']
        self.read_ids.append(artifact_id)
        if artifact_id == self.fail_id:
            raise OSError('injected interrupted receive')
        artifact = self.artifacts[artifact_id]
        if artifact['sha256'] != args['expected_sha256']:
            raise ValueError('wrong hash pin')
        start = args['offset']
        part = self.data[artifact_id][start:start + min(7, args['limit'])]
        end = start + len(part)
        return {'chunk': {'artifact': artifact, 'offset': start, 'encoding': 'HEX', 'raw_bytes': len(part),
                          'data_hex': part.hex(), 'chunk_sha256': hashlib.sha256(part).hexdigest(),
                          'next_offset': end if end < len(self.data[artifact_id]) else None}}


class ReceiverRegression(unittest.TestCase):
    def test_manifest_identity_scope_and_metadata_are_not_substituted(self):
        job, data = fixture()
        with tempfile.TemporaryDirectory() as root:
            client = Client(job, data)
            dest = Path(root) / 'valid'
            result = receiver['receive_job'](client, job, dest)
            self.assertTrue(result['manifest_references_followed'])
            self.assertEqual(set(client.read_ids), set(data))
            self.assertEqual({p.name for p in dest.iterdir()}, {'manifest.json', 'review.json', 'ledger.json.gz'})
            for label, corrupt in [('wrong-run', lambda j: j['job']['payload'].update(run_id='run-other')),
                                   ('wrong-scope', lambda j: j['job']['payload'].update(market='KRW-BTC')),
                                   ('wrong-id', lambda j: j['artifacts'][0].update(artifact_id='artifact-substitute'))]:
                changed = copy.deepcopy(job)
                corrupt(changed)
                rejected = Path(root) / label
                with self.subTest(label=label), self.assertRaises(ValueError):
                    receiver['receive_job'](Client(job, data), changed, rejected)
                self.assertFalse(rejected.exists())
            self.assertFalse(list(Path(root).glob('.backcraft-receiving-*')))

    def test_failed_receive_retry_and_publication_races_preserve_output(self):
        job, data = fixture()
        with tempfile.TemporaryDirectory() as root:
            dest = Path(root) / 'retry'
            with self.assertRaises(OSError):
                receiver['receive_job'](Client(job, data, fail_id='artifact-ledger'), job, dest)
            self.assertFalse(dest.exists())
            receiver['receive_job'](Client(job, data), job, dest)
            publish = receiver['_publish_package']
            raced = Path(root) / 'raced'
            inode = []
            def concurrent_directory(staging, destination):
                destination.mkdir()
                inode.append(destination.stat().st_ino)
                return publish(staging, destination)
            with patch.dict(receiver['receive_job'].__globals__, {'_publish_package': concurrent_directory}), self.assertRaises(FileExistsError):
                receiver['receive_job'](Client(job, data), job, raced)
            self.assertEqual(raced.stat().st_ino, inode[0])
            self.assertEqual(list(raced.iterdir()), [])
            rolled_back = Path(root) / 'rollback'
            real_link = receiver['os'].link
            def interrupted_link(source, target):
                if Path(target).parent == rolled_back and Path(target).name == 'review.json':
                    raise OSError('injected publication failure')
                return real_link(source, target)
            with patch.object(receiver['os'], 'link', side_effect=interrupted_link), self.assertRaises(OSError):
                receiver['receive_job'](Client(job, data), job, rolled_back)
            self.assertFalse(rolled_back.exists())
            replaced = Path(root) / 'replaced'
            def replaced_link(source, target):
                if Path(target).parent == replaced and Path(target).name == 'review.json':
                    ledger = replaced / 'ledger.json.gz'
                    ledger.unlink()
                    ledger.write_bytes(b'concurrent replacement')
                    raise OSError('injected publication failure after replacement')
                return real_link(source, target)
            with patch.object(receiver['os'], 'link', side_effect=replaced_link), self.assertRaises(OSError):
                receiver['receive_job'](Client(job, data), job, replaced)
            self.assertEqual(list(replaced.iterdir()), [replaced / 'ledger.json.gz'])
            self.assertEqual((replaced / 'ledger.json.gz').read_bytes(), b'concurrent replacement')
            self.assertFalse(list(Path(root).glob('.backcraft-receiving-*')))

    def test_legacy_reception_explicitly_reports_catalog_usage(self):
        job, data = fixture('spot-lab-export-v3')
        with tempfile.TemporaryDirectory() as root:
            result = receiver['receive_job'](Client(job, data), job, Path(root) / 'legacy')
            self.assertFalse(result['manifest_references_followed'])
            self.assertIn('Legacy manifest IDs', result['warning'])


if __name__ == '__main__':
    unittest.main()
