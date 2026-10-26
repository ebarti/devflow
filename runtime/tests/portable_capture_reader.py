"""Owned test-only reader for synthetic retained-closure capture.

The production historical Homebrew reader remains unchanged. This fixture
verifies the separately pinned official CLI and release metadata, then injects
its reader verifier into the owned capture process. It exercises downstream
capture custody but does not qualify production historical reader recognition.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path

from devflow_temporal.delivery_temporal_prerequisites import TOOLS


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _verified_metadata(receipt, metadata):
    pin = TOOLS['cli']
    record = receipt['tools']['cli']
    expected_url = (f"https://github.com/{pin['repository']}/releases/download/"
                    f"{pin['tag']}/{pin['archive']}")
    if (receipt.get('schema') != 'jobctrl.temporal-test-prerequisites/v1'
            or record.get('official_repository') != pin['repository']
            or record.get('pinned_tag') != pin['tag']
            or record.get('asset_name') != pin['archive']
            or record.get('asset_url') != expected_url
            or record.get('published_digest') != {'algorithm': 'sha256',
                                                  'value': pin['archive_sha256']}
            or record.get('archive', {}).get('sha256') != pin['archive_sha256']
            or record.get('extraction', {}).get('exact_member') != pin['member']
            or record.get('binary', {}).get('sha256') != pin['binary_sha256']
            or record.get('binary', {}).get('size') != pin['binary_size']):
        raise ValueError('fixture Temporal provenance differs from the pinned official release')
    assets = [entry for entry in metadata.get('assets', []) if entry.get('name') == pin['archive']]
    if (len(assets) != 1 or assets[0].get('digest') != 'sha256:' + pin['archive_sha256']
            or assets[0].get('browser_download_url') != expected_url
            or assets[0].get('size') != record.get('archive', {}).get('size')):
        raise ValueError('fixture Temporal release metadata differs from its official pin')
    return pin


def prepare(root, stage):
    """Create a private test reader from the already downloaded official fixture."""
    root, stage = Path(root), Path(stage)
    source = stage / 'tools/cli/temporal'
    receipt_path = stage / 'temporal-prerequisites.provenance.v1.json'
    metadata_path = stage / 'cli-release.json'
    receipt_raw, metadata_raw = receipt_path.read_bytes(), metadata_path.read_bytes()
    receipt, metadata = json.loads(receipt_raw), json.loads(metadata_raw)
    pin = _verified_metadata(receipt, metadata)
    entries = receipt.get('acquisition_facts', {}).get('official_metadata', [])
    matches = [entry for entry in entries if Path(entry.get('path', '')).name == metadata_path.name]
    archive = stage / pin['archive']
    record = receipt['tools']['cli']
    if (len(matches) != 1 or matches[0].get('path') != str(metadata_path)
            or matches[0].get('sha256') != hashlib.sha256(metadata_raw).hexdigest()
            or record['binary'].get('path') != str(source)
            or record['archive'].get('path') != str(archive)
            or archive.resolve(strict=True) != archive
            or archive.stat().st_size != record['archive'].get('size')
            or sha(archive) != pin['archive_sha256']
            or source.resolve(strict=True) != source
            or source.stat().st_size != pin['binary_size'] or sha(source) != pin['binary_sha256']):
        raise ValueError('fixture source executable or release metadata changed')
    binary = root / 'bin/temporal'
    if not root.exists():
        (root / 'bin').mkdir(mode=0o700, parents=True)
        (root / '.brew').mkdir(mode=0o700)
        shutil.copyfile(source, binary)
        binary.chmod(0o555)
        # The capture owner pins these two legacy locations. Their bytes are
        # explicitly synthetic fixture provenance and official release metadata,
        # not a Homebrew installation receipt or formula.
        (root / 'INSTALL_RECEIPT.json').write_bytes(receipt_raw)
        (root / '.brew/temporal.rb').write_bytes(metadata_raw)
    _check_files(root, pin)
    if ((root / 'INSTALL_RECEIPT.json').read_bytes() != receipt_raw
            or (root / '.brew/temporal.rb').read_bytes() != metadata_raw):
        raise ValueError('fixture reader provenance was replaced')
    result = subprocess.run([str(binary), '--version'], capture_output=True, text=True,
                            timeout=10, check=True)
    if result.stdout.strip() != 'temporal version 1.9.1 (Server 1.32.0, UI 2.54.1)':
        raise ValueError('fixture Temporal executable reports an unexpected version')
    return binary


def _check_files(root, pin):
    binary = root / 'bin/temporal'
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise ValueError('fixture reader root is not canonical')
    for path in (binary, root / 'INSTALL_RECEIPT.json', root / '.brew/temporal.rb'):
        info = path.lstat()
        if (path.resolve(strict=True) != path or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid() or info.st_nlink != 1):
            raise ValueError('fixture reader input is not an owned regular file')
    if (binary.stat().st_size != pin['binary_size'] or sha(binary) != pin['binary_sha256']
            or stat.S_IMODE(binary.stat().st_mode) != 0o555):
        raise ValueError('fixture reader executable does not match the official pin')


def reader(config, declared):
    """Test-only replacement for the historical reader's authorization check."""
    from devflow_temporal import delivery_retained_closures as owner

    binary = Path(os.environ['DEVFLOW_OWNED_CAPTURE_READER'])
    root = binary.parent.parent
    receipt_path, metadata_path = root / 'INSTALL_RECEIPT.json', root / '.brew/temporal.rb'
    receipt_raw, metadata_raw = receipt_path.read_bytes(), metadata_path.read_bytes()
    receipt, metadata = json.loads(receipt_raw), json.loads(metadata_raw)
    pin = _verified_metadata(receipt, metadata)
    entries = receipt.get('acquisition_facts', {}).get('official_metadata', [])
    matches = [entry for entry in entries if Path(entry.get('path', '')).name == 'cli-release.json']
    expected = {'configured_path': str(binary), 'resolved_path': str(binary),
                'sha256': pin['binary_sha256'], 'version': '1.9.1', 'server_version': '1.32.0',
                'historical_start_binary_sha256': None}
    if (binary != root / 'bin/temporal' or config.raw.get('temporal_bin') != str(binary)
            or declared != expected or len(matches) != 1
            or matches[0].get('sha256') != hashlib.sha256(metadata_raw).hexdigest()):
        raise ValueError('fixture capture reader authority differs from its official provenance')
    _check_files(root, pin)
    log = owner.private_bytes(config.state_root / 'temporal.log', maximum=16 * 1024**2)
    if owner.READER_HEADER.encode() not in log.splitlines():
        raise ValueError('fixture startup version evidence is absent')
    if sys.version_info[:3] != (3, 12, 13) or version('temporalio') != '1.33.0':
        raise ValueError('fixture capture requires the original Python and SDK versions')
    return {**declared, 'fixture_profile': 'owned-official-temporal-reader-v1',
            'identity': owner._stamp(binary.stat()),
            'fixture_provenance_sha256': hashlib.sha256(receipt_raw).hexdigest(),
            'fixture_release_metadata_sha256': hashlib.sha256(metadata_raw).hexdigest(),
            'fixture_log_sha256': hashlib.sha256(log).hexdigest(),
            'sdk': '1.33.0', 'python': '3.12.13',
            'grpc_receive_limit_bytes': owner.MAX_HISTORY}
