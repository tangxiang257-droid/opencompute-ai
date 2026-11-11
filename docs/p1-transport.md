# P1 tensor transport

Standalone standard-library modules, with no scheduler/worker/API dependencies.

`Envelope(...).encode()` and `decode(bytes, expected=(task, session, epoch))`
implement v1: binary magic/version/header-length/payload-length prefix, bounded
canonical UTF-8 metadata, raw tensor bytes, then SHA-256 covering the complete
prefix, metadata and payload. Maximum metadata is 4096 bytes, payload 16 MiB,
shape rank 16. Only explicitly enumerated fixed-width dtypes are accepted.
Dimension products are checked against the bound before multiplication. The
network reader checks frame bounds before reading a body. Digest integrity is
not computation correctness or standalone origin authentication; TLS supplies
authentication on the wire.

`TLSConfig(certfile, keyfile, cafile, peers={node: sha256_der_certificate})`
requires certificate validation, TLS >=1.2, client certificates and client-side
hostname validation. Each accepted connection binds exactly one configured node
to the actual TLS peer certificate fingerprint. No plaintext or insecure mode is
provided. Use `asyncio.start_server(handler, ssl=config.context(server=True))`,
then `accept(reader, writer, config, grants, ...)` in the handler; use
`connect(host, port, config, grants, server_hostname=...)` on clients.

Grants are local maps `{authenticated_node: {Grant(...)}}`; both incoming and
outgoing tensors must match task, session, exact execution epoch, stage, model
hash and layout version, and fit the authorized token interval. Provision these
maps from trusted orchestration, never from messages from the remote node.
The Channel/accept boundary also checks the actual stream context uses
CERT_REQUIRED, negotiated TLS 1.2/1.3, and hostname verification for client-side
streams; supplying an unrelated insecure SSL context fails closed. Certificate
issuance, key storage and rotation remain deployment responsibilities.

`Transport` is the replacement interface. `send()` transmits over one persistent
connection. `receive()` yields a `Delivery`; call `release()` or its `commit()`
alias only when the real consumer has released its buffer/resource. Network
receipt never returns credits. Retaining a Delivery intentionally retains its
byte and tile quotas. Calling release twice fails. Receiving all data does not
constitute committing model/KV state.

Each peer advertises bounded byte and tile capacities. Sending consumes both;
explicit release returns both. Sequenced credit messages reject duplicates,
excess and out-of-order updates. The reader processes control messages without
waiting for the consumer; its bounded queue cannot block control progress.
Metadata overhead is additionally bounded per tile. The write lock and bounded
payloads bound the active transport write; control frames have fixed bounded
size. TLS/TCP still has head-of-line blocking; drain/read/credit waits time out.
Disconnect wakes blocked credit and receive waiters. Cancellation before credit
reservation leaves quotas unchanged; cancellation during a write closes the
connection rather than reusing potentially transmitted credits. Cancelling a
release after its accounting changes also aborts the connection, including while
waiting for the write lock; the delivery cannot be retried on that connection.
Close cancels
the reader. Reconnect creates fresh channel accounting; replay deduplication and
cross-connection commitment are higher-layer responsibilities.

This is fixed-window APFC groundwork, not adaptive GPU feedback, NAT traversal,
relay budgeting, retransmission recovery, or a benchmark of home networks.
Tests create ephemeral self-signed certificates with OpenSSL, run real loopback
TLS streams, and never commit private keys.

Run: `python -m unittest discover -s tests -p 'test_p1_*.py' -v`.
