# Export failure retention and bounded bundle metadata

This focused patch hardens existing export/inspection paths. It does not add a
new export mode, enable the private controlled-export gate, deploy anything, or
change public function signatures. It is not the full experimental candidate.

## Intentional behavior changes

`export_local` and `export_chunked` no longer recursively delete their output
path after a failure or interruption. Even when a directory was originally
created exclusively, another actor can replace that path or add unrelated files.
Failures preserve the original exception and cause, and attach a fixed note
warning that output may remain and has not been automatically cleaned. Partial
outputs can now consume disk space until a person inspects them. Do not assume
an unsuccessful export left no files, retry into that directory, or blindly
remove its contents. Successful export behavior is unchanged by this patch.

`inspect_bundle` reads at most 16,385 manifest bytes and enforces the existing
16,384-byte cap. The same bounded bytes supply the parsed object, returned length
and SHA-256. A shared standard-library-only JSON decoder rejects duplicate decoded
keys at any depth, invalid UTF-8/BOM, non-object roots and nonfinite numbers.
Ordinary noncanonical whitespace, Unicode and finite JSON remain supported.
Malformed/ambiguous metadata previously accepted may now fail deliberately.

This does not establish atomic consistency across all bundle files or later
consumer reads. Path checks still race with changes; trusted, stable local files
remain required. A hash does not prove model origin, and checking metadata does
not make serialized tensor data safe. No automatic recovery is introduced.

## Configuration checked versus configuration exported

`export_local` now receives an independent deep copy of the configuration from
its internal directory-inspection call instead of reopening `config.json` after
inspection. Both v1 and v2 consume those checked configuration values. The public
`inspect_directory` signature and result fields are unchanged; the private copy
is not included in CLI JSON or the public inspection result. Inspection failure
still prevents exporter invocation. This closes one configuration check/use gap,
not the lazy weight-file races or whole-directory snapshot consistency.

## Shared supported Qwen2 configuration semantics

The existing `qwen2_shards.manifest_from_config` entry delegates to one pure
standard-library parser. Its public arguments and valid manifest result format
remain unchanged. Dimensions and worker counts must be positive exact integers
(not booleans, strings or fractions), switches must be booleans, and epsilon/theta
must be numeric non-booleans. Explicit zero/false/null head_dim is invalid;
omission still derives hidden/heads. Huge numeric conversion overflow is reported
as ValueError, not OverflowError. Missing dimensions now report ValueError.

Only full attention is supported: layer_types may be absent or a list of exactly
one full_attention value per layer. Null, unknown or sliding layer declarations
reject. Existing default-RoPE and non-sliding restrictions remain. Inactive
sliding_window/max_window_layers metadata does not by itself reject a stock
non-sliding configuration. Unknown metadata is not globally forbidden; this is
not a certification of arbitrary HF configurations or tokenizer/runtime versions.

The current repository call sites are the v1 exporter/synthetic-state helper,
Safetensors planner/inspector, v2 exporter and dense reference function. They
continue using the existing import/entry point. No actual reference or model
function is executed in these checks. The pure parser's validation is not model
numerical correctness, real weight compatibility, weight provenance, or a promise
that every accepted manifest meets the separate bundle consumer's tighter limits.
Callers relying on coercion or malformed optional fields must supply valid types.

## Dependency-free targeted regression commands

From the repository root, using Python 3.11 or newer:

```
python -m unittest discover -s tests -p 'test_export_failure_safety.py' -v
python -m unittest discover -s tests -p 'test_bundle_manifest_json.py' -v
python -m unittest discover -s tests -p 'test_export_config_snapshot.py' -v
python -m unittest discover -s tests -p 'test_qwen2_config_contract.py' -v
```

Six failure-retention tests exercise AST-extracted real export functions using
ordinary object/serializer stand-ins and temporary marker files. Eight bundle
tests exercise the real validator and bounded parser with ordinary marker bytes.
Five configuration tests additionally cover source replacement after inspection,
private deep-copy isolation, unchanged public result/CLI shape, no second read
in the real export function, and inspection failure before export. These use
real function AST with controlled ordinary configuration/reader substitutes.
Ten pure configuration tests cover the strict rejection boundary, a frozen
12-case pre-change valid-output matrix (1/2/4 ranks, tied/untied, GQA/default KV,
default RoPE variants), and static forwarding/import seams. All 29 targeted tests
pass locally; this is not a run of all repository tests.
Coverage includes replacement directories, unrelated files, partial/no output,
interruption, original exception/cause preservation, successful fake output,
duplicate/nested escaped keys, nonfinite JSON, invalid UTF-8/BOM/root, growth,
manifest replacement, historical digest checks and valid noncanonical JSON.
These commands do not import Torch or Safetensors, serialize/deserialize `.pt`,
read weights, construct/run a model, use a GPU or start a Worker/node.

They are not full repository CI, real serializer compatibility, numerical parity,
real model validation, malicious-writer resistance or crash-durability evidence.
The existing broad workflows include model and distributed-runtime tests outside
this narrowly authorized check. Do not infer those ran or passed from this patch.
No workflow or security setting is changed here.
