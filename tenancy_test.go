package reapitest

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	_ "modernc.org/sqlite"
)

// BuildBuddy validates API keys by exact length; a value of any other size is
// rejected as malformed before the group lookup ever happens.
const apiKeyLength = 20

// tenant is one organization plus the API key its traffic is signed with. An
// empty apiKey means unauthenticated.
type tenant struct {
	name    string
	groupID string
	apiKey  string
}

// ctx stamps exactly one identity onto parent, replacing any already there.
// Appending would leave two x-buildbuddy-api-key values on the wire and make it
// ambiguous which one the server honours, which would quietly invalidate every
// isolation assertion below.
func (t tenant) ctx(parent context.Context) context.Context {
	md := metadata.MD{}
	if t.apiKey != "" {
		md.Set("x-buildbuddy-api-key", t.apiKey)
	}
	return metadata.NewOutgoingContext(parent, md)
}

// seedTenants writes organizations and their API keys straight into
// BuildBuddy's database. Going through the app's own API would require an
// authenticated admin, which requires an identity provider, which is exactly
// what a hermetic test cannot have. The server picks up new rows immediately,
// so this works against an already-running app.
func seedTenants(t *testing.T, dataSource string, tenants []tenant) {
	t.Helper()
	db, err := sql.Open("sqlite", dataSource)
	if err != nil {
		t.Fatalf("open %s: %v", dataSource, err)
	}
	defer db.Close()

	now := time.Now().UnixMicro()
	for i, tn := range tenants {
		if tn.apiKey == "" {
			continue
		}
		if len(tn.apiKey) != apiKeyLength {
			t.Fatalf("tenant %q API key is %d chars, must be exactly %d", tn.name, len(tn.apiKey), apiKeyLength)
		}
		if _, err := db.Exec(
			"INSERT INTO `Groups` (group_id, name, url_identifier, created_at_usec, updated_at_usec, status) VALUES (?, ?, ?, ?, ?, 1)",
			tn.groupID, tn.name, tn.name, now, now,
		); err != nil {
			t.Fatalf("insert group %q: %v", tn.name, err)
		}
		// perms 48 mirrors the owner read/write bits BuildBuddy sets on the key
		// it bootstraps for itself; a NULL here is rejected at lookup time.
		if _, err := db.Exec(
			"INSERT INTO APIKeys (api_key_id, label, group_id, value, capabilities, perms, created_at_usec, updated_at_usec, expiry_usec) VALUES (?, ?, ?, ?, 1, 48, ?, ?, 0)",
			fmt.Sprintf("AK%018d", i+1), tn.name, tn.groupID, tn.apiKey, now, now,
		); err != nil {
			t.Fatalf("insert API key for %q: %v", tn.name, err)
		}
	}
}

func makeTenants(n int) []tenant {
	tenants := make([]tenant, n)
	for i := range tenants {
		name := fmt.Sprintf("org%d", i)
		// Pad to the exact length BuildBuddy requires.
		key := name + "Key"
		for len(key) < apiKeyLength {
			key += strconv.Itoa(i)
		}
		tenants[i] = tenant{
			name:    name,
			groupID: fmt.Sprintf("GR%018d", i+1),
			apiKey:  key[:apiKeyLength],
		}
	}
	return tenants
}

// --- isolation operations ---------------------------------------------------
//
// These take the tenant list from clients rather than running as a single
// identity, so they can assert what one organization can see of another.

// pickTwo returns two distinct authenticated tenants.
func pickTwo(rng *rand.Rand, tenants []tenant) (tenant, tenant) {
	a := rng.Intn(len(tenants))
	b := rng.Intn(len(tenants) - 1)
	if b >= a {
		b++
	}
	return tenants[a], tenants[b]
}

// casIsolationOp is the core guarantee: one organization's blobs must be
// invisible to every other organization, through every read path.
var casIsolationOp = operation{name: "CASIsolation", weight: 10, run: func(ctx context.Context, rng *rand.Rand, c *clients) error {
	owner, other := pickTwo(rng, c.tenants)
	secret := randomBytes(rng, rng.Intn(8*1024)+1)
	d := digest(secret)

	if _, err := upload(owner.ctx(ctx), c.cas, secret); err != nil {
		return fmt.Errorf("%s upload: %w", owner.name, err)
	}

	// Positive control. Without this, a server that answered NotFound for
	// everything would satisfy the isolation checks below vacuously.
	read, err := c.cas.BatchReadBlobs(owner.ctx(ctx), &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{d}})
	if err != nil {
		return fmt.Errorf("%s read back: %w", owner.name, err)
	}
	if len(read.Responses) != 1 || !bytes.Equal(read.Responses[0].Data, secret) {
		return fmt.Errorf("%s cannot read back its own blob", owner.name)
	}

	otherCtx := other.ctx(ctx)
	missing, err := c.cas.FindMissingBlobs(otherCtx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{d}})
	if err != nil {
		return fmt.Errorf("%s FindMissingBlobs: %w", other.name, err)
	}
	if len(missing.GetMissingBlobDigests()) != 1 {
		return fmt.Errorf("LEAK: %s sees %s's blob via FindMissingBlobs", other.name, owner.name)
	}

	cross, err := c.cas.BatchReadBlobs(otherCtx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{d}})
	if err == nil {
		for _, r := range cross.Responses {
			if codes.Code(r.Status.GetCode()) == codes.OK && len(r.Data) > 0 {
				return fmt.Errorf("LEAK: %s read %d bytes of %s's blob via BatchReadBlobs", other.name, len(r.Data), owner.name)
			}
		}
	}

	stream, err := c.bs.Read(otherCtx, &bspb.ReadRequest{ResourceName: readResource(d)})
	if err == nil {
		got, readErr := readAll(stream)
		if readErr == nil && len(got) > 0 {
			return fmt.Errorf("LEAK: %s read %d bytes of %s's blob via ByteStream", other.name, len(got), owner.name)
		}
	}
	return nil
}}

// acIsolationOp is the same guarantee for the Action Cache, where a leak is
// worse: a cross-tenant hit hands over another organization's build outputs.
var acIsolationOp = operation{name: "ACIsolation", weight: 10, run: func(ctx context.Context, rng *rand.Rand, c *clients) error {
	owner, other := pickTwo(rng, c.tenants)
	ownerCtx := owner.ctx(ctx)

	commandBytes, err := proto.Marshal(&repb.Command{Arguments: []string{"build", fmt.Sprint(rng.Uint64())}})
	if err != nil {
		return err
	}
	digests, err := upload(ownerCtx, c.cas, commandBytes)
	if err != nil {
		return fmt.Errorf("%s upload: %w", owner.name, err)
	}
	actionBytes, err := proto.Marshal(&repb.Action{CommandDigest: digests[0]})
	if err != nil {
		return err
	}
	actionDigests, err := upload(ownerCtx, c.cas, actionBytes)
	if err != nil {
		return err
	}
	actionDigest := actionDigests[0]

	want := int32(rng.Intn(200) + 1)
	if _, err := c.ac.UpdateActionResult(ownerCtx, &repb.UpdateActionResultRequest{
		ActionDigest: actionDigest,
		ActionResult: &repb.ActionResult{ExitCode: want},
	}); err != nil {
		return fmt.Errorf("%s UpdateActionResult: %w", owner.name, err)
	}

	// Positive control.
	got, err := c.ac.GetActionResult(ownerCtx, &repb.GetActionResultRequest{ActionDigest: actionDigest})
	if err != nil {
		return fmt.Errorf("%s cannot read back its own action result: %w", owner.name, err)
	}
	if got.ExitCode != want {
		return fmt.Errorf("%s read back exit_code %d, want %d", owner.name, got.ExitCode, want)
	}

	leaked, err := c.ac.GetActionResult(other.ctx(ctx), &repb.GetActionResultRequest{ActionDigest: actionDigest})
	if err == nil {
		return fmt.Errorf("LEAK: %s read %s's action result (exit_code %d)", other.name, owner.name, leaked.GetExitCode())
	}
	if code := status.Code(err); code != codes.NotFound {
		return fmt.Errorf("%s got %s reading %s's action result, want NotFound", other.name, code, owner.name)
	}
	return nil
}}

// rejectedIdentityOp checks that identities the server should refuse are
// refused on every service, rather than silently degrading to some other
// tenant's view.
var rejectedIdentityOp = operation{name: "RejectedIdentity", weight: 10, run: func(ctx context.Context, rng *rand.Rand, c *clients) error {
	// A malformed key, a well-formed key that was never issued, and no key at
	// all. Anonymous access is disabled for this test, so all three must fail.
	candidates := []tenant{
		{name: "malformed", apiKey: "too-short"},
		{name: "unissued", apiKey: "zzzzUnissuedKeyzzzzz"},
		{name: "no-key", apiKey: ""},
	}
	bad := candidates[rng.Intn(len(candidates))]
	badCtx := bad.ctx(ctx)

	data := randomBytes(rng, 128)
	d := digest(data)

	if _, err := c.cas.FindMissingBlobs(badCtx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{d}}); err == nil {
		return fmt.Errorf("FindMissingBlobs succeeded for the %s identity", bad.name)
	} else if code := status.Code(err); code != codes.Unauthenticated && code != codes.PermissionDenied {
		return fmt.Errorf("FindMissingBlobs for %s = %s, want Unauthenticated", bad.name, code)
	}

	if _, err := c.cas.BatchUpdateBlobs(badCtx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: d, Data: data}},
	}); err == nil {
		return fmt.Errorf("BatchUpdateBlobs succeeded for the %s identity", bad.name)
	}

	if _, err := c.ac.GetActionResult(badCtx, &repb.GetActionResultRequest{ActionDigest: d}); err == nil {
		return fmt.Errorf("GetActionResult succeeded for the %s identity", bad.name)
	} else if code := status.Code(err); code != codes.Unauthenticated && code != codes.PermissionDenied {
		return fmt.Errorf("GetActionResult for %s = %s, want Unauthenticated", bad.name, code)
	}
	return nil
}}

// sharedDigestOp has two organizations store byte-identical content. Whatever
// deduplication happens underneath, neither may observe the other's copy, and
// both must still read their own bytes back intact.
var sharedDigestOp = operation{name: "SharedDigest", weight: 10, run: func(ctx context.Context, rng *rand.Rand, c *clients) error {
	first, second := pickTwo(rng, c.tenants)
	shared := randomBytes(rng, rng.Intn(4*1024)+1)
	d := digest(shared)

	if _, err := upload(first.ctx(ctx), c.cas, shared); err != nil {
		return fmt.Errorf("%s upload: %w", first.name, err)
	}
	// The second organization must still be told it is missing, even though an
	// identical blob already exists under the first.
	missing, err := c.cas.FindMissingBlobs(second.ctx(ctx), &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{d}})
	if err != nil {
		return fmt.Errorf("%s FindMissingBlobs: %w", second.name, err)
	}
	if len(missing.GetMissingBlobDigests()) != 1 {
		return fmt.Errorf("LEAK: %s sees identical content uploaded by %s", second.name, first.name)
	}
	if _, err := upload(second.ctx(ctx), c.cas, shared); err != nil {
		return fmt.Errorf("%s upload: %w", second.name, err)
	}
	// Both must now read their own copy correctly.
	for _, tn := range []tenant{first, second} {
		read, err := c.cas.BatchReadBlobs(tn.ctx(ctx), &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{d}})
		if err != nil {
			return fmt.Errorf("%s read: %w", tn.name, err)
		}
		if len(read.Responses) != 1 || !bytes.Equal(read.Responses[0].Data, shared) {
			return fmt.Errorf("%s cannot read back shared-digest content", tn.name)
		}
	}
	return nil
}}

var isolationOperations = []operation{
	casIsolationOp,
	acIsolationOp,
	rejectedIdentityOp,
	sharedDigestOp,
}

func TestMultiTenantIsolation(t *testing.T) {
	seed := time.Now().UnixNano()
	if value := os.Getenv("REAPI_SEED"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatalf("invalid REAPI_SEED: %v", err)
		}
		seed = parsed
	}
	t.Logf("random seed: %d (reproduce with --test_env=REAPI_SEED=%d)", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	iterations := 50
	if value := os.Getenv("REAPI_REQUESTS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			t.Fatalf("invalid REAPI_REQUESTS %q", value)
		}
		iterations = parsed
	}

	// self_auth turns on API key checking without an external identity
	// provider; without it BuildBuddy configures no authenticator at all and
	// every key, valid or not, is ignored. Anonymous access is off so that
	// unauthenticated traffic must fail rather than fall back to a shared
	// tenant.
	inst := startBuildBuddy(t, withFlags(
		"--auth.enable_self_auth=true",
		"--auth.enable_anonymous_usage=false",
	))
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", inst.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	awaitReadyAuthOptional(t, conn, inst.exited)

	tenants := makeTenants(4)
	seedTenants(t, inst.dataSource, tenants)

	c := &clients{
		cas:     repb.NewContentAddressableStorageClient(conn),
		ac:      repb.NewActionCacheClient(conn),
		bs:      bspb.NewByteStreamClient(conn),
		caps:    repb.NewCapabilitiesClient(conn),
		tenants: tenants,
	}

	capCtx, capCancel := context.WithTimeout(tenants[0].ctx(context.Background()), requestTimeout)
	c.server, err = c.caps.GetCapabilities(capCtx, &repb.GetCapabilitiesRequest{})
	capCancel()
	if err != nil {
		t.Fatalf("GetCapabilities as %s: %v", tenants[0].name, err)
	}

	// Confirm the seeded identities actually authenticate before asserting
	// anything about isolation; otherwise a broken seed looks like perfect
	// isolation.
	for _, tn := range tenants {
		ctx, cancel := context.WithTimeout(tn.ctx(context.Background()), requestTimeout)
		_, err := upload(ctx, c.cas, []byte("liveness probe for "+tn.name))
		cancel()
		if err != nil {
			t.Fatalf("tenant %s cannot write with its API key: %v", tn.name, err)
		}
	}

	// Ordinary cache traffic from a random tenant, interleaved with the
	// isolation probes.
	pool := append(append([]operation{}, operations...), isolationOperations...)
	counts := make(map[string]int, len(pool))
	for i := 0; i < iterations; i++ {
		op := pick(rng, pool)
		counts[op.name]++
		actor := tenants[rng.Intn(len(tenants))]
		ctx, cancel := context.WithTimeout(actor.ctx(context.Background()), requestTimeout)
		err := op.run(ctx, rng, c)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d, %s (as %s): %v", i, op.name, actor.name, err)
		}
	}
	t.Logf("completed %d operations across %d tenants: %v", iterations, len(tenants), counts)
}

// awaitReadyAuthOptional waits for the server to answer at all. With anonymous
// access disabled an unauthenticated probe is refused, which still proves the
// server is up and serving.
func awaitReadyAuthOptional(t *testing.T, conn *grpc.ClientConn, exited <-chan struct{}) {
	t.Helper()
	client := repb.NewCapabilitiesClient(conn)
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			t.Fatalf("BuildBuddy exited before becoming ready (last probe: %v)", lastErr)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, lastErr = client.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
		cancel()
		if lastErr == nil || status.Code(lastErr) == codes.Unauthenticated {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("BuildBuddy did not become ready within 30s: %v", lastErr)
}
