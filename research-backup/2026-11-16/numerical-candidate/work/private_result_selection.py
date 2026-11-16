"""Unmounted local historical selection; objects convey no authority."""
from dataclasses import dataclass

@dataclass(frozen=True)
class _Tile:
    tile: int
    start: int
    count: int
    shape: tuple
    bytes: int
    sha256: str

@dataclass(frozen=True)
class _Declaration:
    task: str
    request: str
    session: str
    epoch: int
    model_claim: str
    input_ref: str
    token_sha256: str
    preprocessing_version: str
    preprocessing_sha256: str
    dtype: str
    axes: tuple
    token_count: int
    total_bytes: int
    tiles: tuple
    manifest_digest: str

@dataclass(frozen=True)
class _SelectedResult:
    digest: str
    declaration: _Declaration

@dataclass(frozen=True)
class _HistoricalAssociation:
    selection: _Declaration
    observation: object

# Only called with the already reconstructed, canonical-equal local manifest.
# No caller labels, new lookup, authorization call, callback, I/O or clock.
def _copy_selected(expected, digest):
    tiles=tuple(_Tile(e['tile'],e['start'],e['count'],tuple(e['shape']),e['bytes'],e['sha256']) for e in expected['tiles'])
    return _Declaration(expected['task'],expected['request'],expected['session'],expected['epoch'],
        expected['model_claim'],expected['input_ref'],expected['token_sha256'],
        expected['preprocessing_version'],expected['preprocessing_sha256'],expected['dtype'],
        tuple(expected['axes']),expected['token_count'],expected['total_bytes'],tiles,digest)
