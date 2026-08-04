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

## Multi-tenant isolation

`TestMultiTenantIsolation` runs the whole operation pool again, but as four
distinct organizations rather than one anonymous client, and adds probes for
the guarantee that matters most in a shared cache: no organization may observe
another's content.

Getting there requires two things that are not obvious:

- `--auth.enable_self_auth=true`. Without it BuildBuddy configures no
  authenticator at all and ignores API keys entirely — a garbage key is
  accepted exactly like a valid one. Anonymous usage is also disabled, so
  unauthenticated traffic must fail rather than fall back to a shared tenant.
- Organizations and API keys are seeded directly into the server's SQLite
  database. Creating them through the app's own API needs an authenticated
  admin, which needs an identity provider, which a hermetic test cannot have.
  New rows are picked up immediately, so no restart is needed.

**API key values must be exactly 20 characters.** Any other length is rejected
as malformed before the group lookup happens, which presents as a valid key
being refused. `seedTenants` enforces this so the failure is obvious.

What is asserted:

- A blob written by one organization is invisible to every other through
  `FindMissingBlobs`, `BatchReadBlobs`, and ByteStream `Read`
- An action result written by one organization returns `NOT_FOUND` for every
  other
- Two organizations storing byte-identical content each read back their own
  copy, and neither is told the other's upload already satisfies it
- Malformed keys, well-formed but unissued keys, and absent keys are all
  refused on every service
- Every check is paired with a positive control confirming the owner *can*
  see its own data. Without those, a server that answered `NOT_FOUND` for
  everything would satisfy every isolation assertion vacuously.

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

Both tests live in the same target; run one with `--test_filter`:

```sh
bazel test //:reapi_test --test_filter=TestMultiTenantIsolation
```

Because every port is reserved per instance, `--runs_per_test=N` explores N
independent seeds in parallel.

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

Two things the tests deliberately do **not** assert, because measurement showed
they are not guarantees this version makes:

- **API key `capabilities` do not gate cache access.** A key with
  `capabilities = 0` still writes to both the CAS and the Action Cache, with
  anonymous access disabled, so there is no read-only tier to test on the
  cache path. The capability bits exist (`CACHE_WRITE`, `CAS_WRITE`, …) and
  there is a `missing capability` error path, so they evidently gate something
  else.
- **`instance_name` is not a tenancy boundary.** Within one organization, a
  blob written under one instance name is visible under every other, including
  the empty one. Organization is the isolation boundary; instance name is not.

Finally, BuildBuddy binds eight ports, six of which have fixed defaults (9090
monitoring, 9099 telemetry, 1986-1988 internal, 8081 TLS). The harness pins all
of them to reserved ephemeral ports; leaving them at their defaults means two
instances cannot coexist, which breaks `--runs_per_test`, parallel Bazel jobs,
and any machine that happens to have something on 9090.
