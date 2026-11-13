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

## Dependency-free targeted regression commands

From the repository root, using Python 3.11 or newer:

```
python -m unittest discover -s tests -p 'test_export_failure_safety.py' -v
python -m unittest discover -s tests -p 'test_bundle_manifest_json.py' -v
```

Six failure-retention tests exercise AST-extracted real export functions using
ordinary object/serializer stand-ins and temporary marker files. Eight bundle
tests exercise the real validator and bounded parser with ordinary marker bytes.
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
