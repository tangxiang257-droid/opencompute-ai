"""Bounded, local-trust manifest for a fixed ordinary-byte receipt probe.

The raw hash is supplied by trusted LOCAL assembly, never a peer message. Hashes
and public fingerprints bind declarations; they authenticate neither a peer nor a
model. This module performs no I/O, database creation, TLS or model execution.
"""
import hashlib
import hmac
import json
import re
import struct
from typing import Annotated,Literal
from pydantic import Field,field_validator,model_validator
from compute.bounded_json import parse_json_object
from .contracts import Record,Task,Session,Stage,ResourceLimits,Identifier,Digest,State
from .tile_contract_v2 import TileV2,DescriptorV2,validate_prefill_plan
from .envelope import Envelope

MAX_MANIFEST_BYTES=65536
FIXTURE_LABEL='fixed-probe-bytes-not-model-output'
FIXTURE_SHA256=hashlib.sha256(struct.pack('<f',1.0)*120).hexdigest()

class ProbeManifestRejected(ValueError):
    def __init__(self):super().__init__('unsupported receipt probe manifest')

class ProbeBudget(Record):
    hidden_size:Literal[24]
    vocab_size:Literal[32]
    token_count:Literal[5]
    tile_count:Literal[3]
    deliveries:Literal[4]
    receipts:Literal[3]
    unique_payload_bytes:Literal[480]
    transmitted_payload_bytes:Literal[672]
    capacity_bytes:Literal[65536]
    capacity_tiles:Literal[1]

    @field_validator('*',mode='before')
    @classmethod
    def exact_integer(cls,value):
        if type(value)is not int:raise ValueError('integer budget required')
        return value

class ProbeManifest(Record):
    schema_version:Literal[1]
    task:Task
    session:Session
    stages:Annotated[tuple[Stage,...],Field(min_length=2,max_length=2)]
    tiles:Annotated[tuple[TileV2,...],Field(min_length=6,max_length=6)]
    config_sha256:Digest
    sender_node:Identifier
    receiver_node:Identifier
    sender_certificate_sha256:Digest
    receiver_certificate_sha256:Digest
    expected_server_name:Annotated[str,Field(min_length=1,max_length=253)]
    fixture_sha256:Digest
    budget:ProbeBudget

    @field_validator('schema_version',mode='before')
    @classmethod
    def explicit_version(cls,value):
        if type(value)is not int or value!=1:raise ValueError('integer version required')
        return value

    @model_validator(mode='after')
    def fixed_probe(self):
        if self.sender_node==self.receiver_node:raise ValueError('distinct endpoints required')
        if self.sender_certificate_sha256==self.receiver_certificate_sha256:raise ValueError('distinct role credentials required')
        if self.session.selected_nodes!=(self.sender_node,self.receiver_node):raise ValueError('ordered endpoints required')
        if tuple(x.assigned_node for x in self.stages)!=(self.sender_node,self.receiver_node):raise ValueError('stage endpoints')
        if re.fullmatch('[0-9a-f]{32}',self.session.session_id) is None:raise ValueError('nonce required')
        # DNS-shaped name, not a URL, path, credentials, wildcard or URI authority.
        labels=self.expected_server_name.split('.')
        if any(re.fullmatch('[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?',x) is None for x in labels):raise ValueError('explicit server name required')
        if self.task.payload_ref!=FIXTURE_LABEL:raise ValueError('fixed sample source marker required')
        if self.task.state!=State.QUEUED or self.session.state!=State.PREPARING:raise ValueError('fresh preparing declarations required')
        if self.task.total_layers!=3 or [(s.layer_start,s.layer_end) for s in self.stages]!=[(0,1),(1,3)]:raise ValueError('fixed layer ranges')
        if self.session.expected_tokens!=5:raise ValueError('fixed token count')
        if self.fixture_sha256!=FIXTURE_SHA256:raise ValueError('fixed payload digest')
        digests=[self.config_sha256,self.sender_certificate_sha256,self.receiver_certificate_sha256,self.fixture_sha256,self.task.model_hash,self.session.model_hash,self.session.layout_hash]
        digests.extend(d for s in self.stages for d in (s.model_hash,s.layout_hash))
        digests.extend(d for t in self.tiles for d in (t.model_hash,t.layout_hash))
        if any(re.fullmatch('[0-9a-f]{64}',d) is None for d in digests):raise ValueError('strict digest')
        validate_prefill_plan(self.task,self.session,self.stages,self.tiles,hidden_size=24,vocab_size=32)
        for i,(start,count) in enumerate(((0,2),(2,2),(4,1))):
            if any((t.tile_id,t.token_start,t.token_count)!=(i,start,count) for t in self.tiles[2*i:2*i+2]):raise ValueError('fixed sample tiles')
        return self


def _complete(obj,model):
    if type(obj)is not dict or set(obj)!=set(model.model_fields):raise ValueError('complete explicit fields required')


def validate_probe_manifest(raw_bytes,*,trusted_manifest_sha256):
    """Validate exact raw SHA from trusted local configuration, then strict JSON.

    Every record field including normally-defaulted Task/ResourceLimits/Session
    fields must be explicit. No self-reported hash, source tag, signature, path,
    remote permit or online manifest negotiation is accepted as authority.
    """
    try:
        if type(trusted_manifest_sha256)is not str or re.fullmatch('[0-9a-f]{64}',trusted_manifest_sha256) is None:raise ValueError()
        if type(raw_bytes)is not bytes or not 0<len(raw_bytes)<=MAX_MANIFEST_BYTES:raise ValueError()
        if not hmac.compare_digest(hashlib.sha256(raw_bytes).hexdigest(),trusted_manifest_sha256):raise ValueError()
        obj=parse_json_object(raw_bytes,MAX_MANIFEST_BYTES)
        _complete(obj,ProbeManifest);_complete(obj['task'],Task);_complete(obj['task']['resource_limits'],ResourceLimits)
        _complete(obj['session'],Session);_complete(obj['budget'],ProbeBudget)
        if type(obj['stages'])is not list or len(obj['stages'])!=2:raise ValueError()
        if type(obj['tiles'])is not list or len(obj['tiles'])!=6:raise ValueError()
        for stage in obj['stages']:_complete(stage,Stage)
        for tile in obj['tiles']:
            _complete(tile,TileV2);_complete(tile['input'],DescriptorV2);_complete(tile['output'],DescriptorV2)
        # JSON-mode strict validation preserves supported array->tuple and enum
        # decoding while refusing strings/bools in integer fields.
        return ProbeManifest.model_validate_json(raw_bytes,strict=True)
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError,RecursionError):
        pass
    raise ProbeManifestRejected() from None


def fixture_frames(manifest):
    """Three independent fixed sample Envelopes; no sender-computation claim.

    Fixture digest = SHA256(payload0 || payload1 || payload2), little-endian
    float32 ones, 480 bytes. Repeating frame0 yields the four-delivery 672-byte
    payload sequence. Hash excludes headers and excludes the duplicate payload.
    """
    if type(manifest)is not ProbeManifest:raise ProbeManifestRejected()
    m=ProbeManifest.model_validate(manifest)
    frames=tuple(Envelope(m.task.task_id,m.session.session_id,m.session.epoch,0,tile,(start,start+n),m.task.model_hash,2,(1,n,24),'float32',struct.pack('<f',1.0)*(n*24))
                 for tile,(start,n) in enumerate(((0,2),(2,2),(4,1))))
    # Full wire size includes bounded public identifiers and JSON header, not
    # just the fixed 480-byte payload sum. This is a wire bound, not an RSS cap.
    for frame in frames:
        wire=frame.encode()
        if type(wire)is not bytes or not 1<=len(wire)<=m.budget.capacity_bytes:
            raise ProbeManifestRejected()
    return frames
