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
- A batch mixing present and absent digests must report per-entry statuses
- The empty blob must always be present and readable without being uploaded

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

Every run logs its seed, so a failure can be replayed exactly. 20000 operations
take about 13 seconds.

## Notes on the server under test

The enterprise app starts without a license for this cache-only configuration.

Remote execution is intentionally disabled because its default configuration
requires Redis and separate executor processes; the `Execution` service is
therefore out of scope here, and this test covers the cache half of REAPI.

BuildBuddy v2.290.0 answers `ByteStream.QueryWriteStatus` with `Unimplemented`.
The ByteStream contract permits this, so the resumption test falls back to
restarting the upload from offset zero, which is the documented client
behaviour. If a future version implements the call, the test will begin
verifying the reported `committed_size` instead.
