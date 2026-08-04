# BuildBuddy REAPI outside-in test

This Bazel project downloads the unmodified
`buildbuddy-enterprise-linux-amd64` artifact from BuildBuddy v2.290.0 and tests
its remote cache through the public Remote Execution API. The binary is pinned
by SHA-256; it is not rebuilt from source.

The test starts BuildBuddy with an isolated in-memory cache and SQLite database,
waits for the Capabilities service, then sends randomized traffic drawn from a
pool of operations, validating each round trip before shutting the process down.

## Coverage

Happy paths:

- `Capabilities.GetCapabilities`, asserting SHA-256 is advertised
- CAS `FindMissingBlobs` before and after upload, `BatchUpdateBlobs`,
  `BatchReadBlobs`, and `GetTree` over a nested directory
- ByteStream `Write` in randomly sized chunks, verifying `committed_size`
- ByteStream `Read` with random `read_offset` and `read_limit`
- ByteStream write resumption via `QueryWriteStatus`
- Action Cache `UpdateActionResult` and `GetActionResult` with a populated
  result: exit code, stdout digest, and an output file backed by real CAS
  contents

Error and edge paths, which is where a cache is most likely to be wrong:

- Reading a blob that was never written must fail `NOT_FOUND`
- `GetActionResult` for an uncached action must fail `NOT_FOUND`
- `BatchUpdateBlobs` must reject data that does not match its digest
- A ByteStream write whose bytes do not hash to the digest in its resource
  name must not become readable under that digest
- A batch mixing present and absent digests must report per-entry statuses
- The empty blob must always be present and readable without being uploaded

Boundary and stress paths, driven by the limits the server itself advertises:

- Structurally invalid resource names (empty, non-hex, negative size,
  truncated hash, path traversal) must produce clean gRPC errors
- Reads at and past the end of a blob must never return data
- A `BatchUpdateBlobs` larger than the advertised
  `max_batch_total_size_bytes` must be refused, or every blob it reports `OK`
  must really be stored
- Blobs past the 4 MiB gRPC message limit must survive a ByteStream round
  trip, and `BatchReadBlobs` must not return a short blob under an `OK` status
- A digest repeated within one batch must not break request/response
  correlation
- Concurrent writers and readers of the same digests must never expose a
  partial blob

Operations are weighted, so expensive probes stay rare and a long run remains
dominated by ordinary cache traffic.

## Running

Run the default 50 randomized operations with:

```sh
bazel test //:reapi_test --test_output=errors
```

Increase the request count or replay a logged seed with:

```sh
bazel test //:reapi_test \
  --test_env=REAPI_REQUESTS=20000 \
  --test_env=REAPI_SEED=12345 \
  --test_output=streamed
```

Every run logs its seed, so a failure can be replayed exactly. 25000 operations
take about 40 seconds. The suite is concurrency-bearing, so it is worth running
under the race detector periodically:

```sh
bazel test //:reapi_test --@rules_go//go/config:race --test_env=REAPI_REQUESTS=2000
```

## Notes on the server under test

The enterprise app starts without a license for this cache-only configuration.

Remote execution is intentionally disabled because its default configuration
requires Redis and separate executor processes; the `Execution` service is
therefore out of scope here, and this test covers the cache half of REAPI.

Two behaviours of v2.290.0 the test accommodates rather than asserts against:

`ByteStream.QueryWriteStatus` returns `Unimplemented`. The ByteStream contract
permits this, so the resumption test falls back to restarting the upload from
offset zero, which is the documented client behaviour. If a future version
implements the call, the test will begin verifying the reported
`committed_size` instead.

A `Read` with `read_offset` past the end of a blob returns `OK` with an empty
stream. ByteStream says this "must return an `OUT_OF_RANGE` error", so this is
a genuine deviation, though a harmless one: no data is exposed. The practical
cost is that a client cannot distinguish an empty tail from an impossible
offset, which matters for resumable downloads that use offsets to detect
truncation. The test tolerates the empty result and still fails if any bytes
come back past the end.

BuildBuddy also ends a ByteStream write early when the blob already exists.
gRPC surfaces that to the client as `io.EOF` from `Send`, with the real status
on `CloseAndRecv` — worth knowing, because treating that `io.EOF` as a failure
makes concurrent writers of the same digest look broken when they are not.
