# Explicit input encoding v2: LE, C order, batch one

`decode_input_payload` lives in the existing TileV2 contract module and revalidates
that actual TileV2 input descriptor. It returns an immutable bounded flat tuple of
ordinary Python values plus shape/dtype, not a Tensor or execution capability.
The actual Receiver opt-in calls this same decoder before inserting any receipt.
No Tensor/prefill, model, node or network experiment is performed.

Encoding identifier: `p1-input-v2-le-c-batch1`, input encoding version exactly 2.
This is a new experimental encoding, not a reinterpretation of the old raw-byte
protocol. It is independent of Envelope.layout_version (deployment layout). A
Receiver must explicitly receive BOTH input_layout_version=2 and a trusted
input_layout_wire_version matching its EdgeBinding.layout_version, with wire
version >=2. No default mapping is inferred and legacy wire version 1 rejects.
The exact mapping/name is persisted in the existing edge body; an edge previously
registered as opaque cannot be silently upgraded, even for identical bytes.
Old Receiver defaults and existing frames remain opaque, not numeric-certified.
The frame protocol version itself remains unchanged; trusted control-plane mapping
is required because the wire header does not carry the new input-encoding name.

## Exact value rules
- batch=1; flattening is C row-major (last axis varies fastest), no padding bytes.
- stage 0 input: int64 little-endian, shape [1,N], each token 0 <= x < the explicitly
  supplied trusted vocabulary size, with vocabulary bound 1..262144.
- stage 1 input: IEEE754 binary32 little-endian, shape [1,N,H], H <=4096, finite
  values only. NaN/+Inf/-Inf reject; signed zeros and finite subnormals are valid.
- Tile input role/dtype/rank and shape token axis must match its declared stage and
  token_count. The counterpart output descriptor is still validated by TileV2.
- Strings, bool/fractional versions, mutable bytearrays/views and absent/unknown
  encoding versions reject. There is no native-endian or native-alignment path.

Wrong-endian input cannot always be detected: a mislabeled big-endian float can
also be a valid small little-endian float. The decoder follows trusted declared
encoding and never guesses. Likewise shape/row ordering must be truthful; arbitrary
finite values do not prove the sender used the correct model/order. Golden tests
check known byte/value pairs and explicitly retain this limitation.

## Resource and operator scope
The existing 64 KiB input cap is not raised; Receiver also retains its stricter
64 KiB WHOLE-frame cap, so a maximum-size standalone payload will not fit that
frame. Expected byte length is bounded before multiplication/decoding; at most
16384 numeric values (8192 int64 values) are allocated. The Python tuple/scalars
consume additional bounded application memory, not an RSS or OS memory guarantee.
No streaming, implicit truncation, output logits compression or quota bypass.

Pure decode does not prove valid context positions, actual model identity or
operator numerical correctness. Full receiver plan checks still bind token ranges,
hidden width/vocabulary declarations, epoch/model/layout and adjacent descriptors.
Real #17 prefill additionally checks device, Tensor dtype, finite values, context
capacity and KV order. This code does NOT create that Tensor or certify the checks
against a running model. Future conversion must explicitly preserve this encoding
and validate independently owned Tensor lifetime. No execution gate is opened.
