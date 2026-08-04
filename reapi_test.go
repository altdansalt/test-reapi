package reapitest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const requestTimeout = 10 * time.Second

func digest(data []byte) *repb.Digest {
	sum := sha256.Sum256(data)
	return &repb.Digest{Hash: hex.EncodeToString(sum[:]), SizeBytes: int64(len(data))}
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	_, _ = rng.Read(b)
	return b
}

// reservePort binds an ephemeral port and keeps it held until release is
// called, so the window between choosing a port and BuildBuddy binding it is
// as small as we can make it without letting the server pick.
func reservePort(t *testing.T) (port int, release func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l.Addr().(*net.TCPAddr).Port, func() { _ = l.Close() }
}

// startBuildBuddy launches the server and returns its gRPC port plus a channel
// closed when the process exits, so callers can distinguish "still starting"
// from "already dead".
func startBuildBuddy(t *testing.T) (int, <-chan struct{}) {
	t.Helper()
	app := os.Getenv("BUILDBUDDY_BINARY")
	if app == "" {
		t.Fatal("BUILDBUDDY_BINARY is not set")
	}
	state := t.TempDir()
	grpcPort, releaseGRPC := reservePort(t)
	httpPort, releaseHTTP := reservePort(t)
	args := []string{
		"--grpc_port=" + strconv.Itoa(grpcPort),
		"--port=" + strconv.Itoa(httpPort),
		"--cache.in_memory=true",
		"--cache.max_size_bytes=268435456",
		"--remote_execution.enable_remote_exec=false",
		"--auth.enable_anonymous_usage=true",
		"--disable_telemetry=true",
		"--database.data_source=sqlite3://" + filepath.Join(state, "buildbuddy.db"),
		// Blobstore for invocation artifacts; distinct from the cache above and
		// defaulted to a shared /tmp path, so it must be pinned per test.
		"--storage.disk.root_directory=" + filepath.Join(state, "storage"),
		"--app.log_level=warn",
	}
	cmd := exec.Command(app, args...)
	logPath := filepath.Join(state, "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	releaseGRPC()
	releaseHTTP()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start BuildBuddy: %v", err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
		_ = logFile.Close()
		if t.Failed() {
			if contents, err := os.ReadFile(logPath); err == nil {
				t.Logf("BuildBuddy log:\n%s", contents)
			}
		}
	})
	return grpcPort, exited
}

func awaitReady(t *testing.T, conn *grpc.ClientConn, exited <-chan struct{}) {
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
		if lastErr == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("BuildBuddy did not become ready within 30s: %v", lastErr)
}

type clients struct {
	cas  repb.ContentAddressableStorageClient
	ac   repb.ActionCacheClient
	bs   bspb.ByteStreamClient
	caps repb.CapabilitiesClient
}

func uploadResource(rng *rand.Rand, d *repb.Digest) string {
	return fmt.Sprintf("uploads/%016x%016x/blobs/%s/%d", rng.Uint64(), rng.Uint64(), d.Hash, d.SizeBytes)
}

func readResource(d *repb.Digest) string {
	return fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes)
}

// upload stores blobs via BatchUpdateBlobs and returns their digests in the
// order given.
func upload(ctx context.Context, cas repb.ContentAddressableStorageClient, blobs ...[]byte) ([]*repb.Digest, error) {
	digests := make([]*repb.Digest, len(blobs))
	reqs := make([]*repb.BatchUpdateBlobsRequest_Request, len(blobs))
	for i, data := range blobs {
		digests[i] = digest(data)
		reqs[i] = &repb.BatchUpdateBlobsRequest_Request{Digest: digests[i], Data: data}
	}
	resp, err := cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{Requests: reqs})
	if err != nil {
		return nil, fmt.Errorf("BatchUpdateBlobs: %w", err)
	}
	if len(resp.Responses) != len(reqs) {
		return nil, fmt.Errorf("BatchUpdateBlobs returned %d responses, want %d", len(resp.Responses), len(reqs))
	}
	for _, r := range resp.Responses {
		if codes.Code(r.Status.GetCode()) != codes.OK {
			return nil, fmt.Errorf("BatchUpdateBlobs(%s): %s", r.Digest.GetHash(), r.Status.GetMessage())
		}
	}
	return digests, nil
}

// readAll drains a ByteStream Read into a single buffer.
func readAll(stream bspb.ByteStream_ReadClient) ([]byte, error) {
	var got []byte
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return got, nil
		}
		if err != nil {
			return nil, err
		}
		got = append(got, msg.Data...)
	}
}

// writeChunked uploads data in randomly sized chunks and verifies the
// committed size the server reports back.
func writeChunked(ctx context.Context, bs bspb.ByteStreamClient, rng *rand.Rand, data []byte) error {
	stream, err := bs.Write(ctx)
	if err != nil {
		return err
	}
	resource := uploadResource(rng, digest(data))
	for offset := 0; ; {
		chunk := rng.Intn(16*1024) + 1
		if offset+chunk > len(data) {
			chunk = len(data) - offset
		}
		last := offset+chunk == len(data)
		req := &bspb.WriteRequest{
			WriteOffset: int64(offset),
			Data:        data[offset : offset+chunk],
			FinishWrite: last,
		}
		// The resource name is only required on the first request of a stream.
		if offset == 0 {
			req.ResourceName = resource
		}
		if err := stream.Send(req); err != nil {
			return fmt.Errorf("Send at offset %d: %w", offset, err)
		}
		offset += chunk
		if last {
			break
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("CloseAndRecv: %w", err)
	}
	if resp.CommittedSize != int64(len(data)) {
		return fmt.Errorf("committed_size = %d, want %d", resp.CommittedSize, len(data))
	}
	return nil
}

// --- operations -------------------------------------------------------------

type operation struct {
	name string
	run  func(ctx context.Context, rng *rand.Rand, c *clients) error
}

// capabilitiesOp asserts the server advertises the digest function this test
// actually uses, rather than discarding the response.
var capabilitiesOp = operation{"Capabilities", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	resp, err := c.caps.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	if err != nil {
		return err
	}
	cache := resp.GetCacheCapabilities()
	if cache == nil {
		return errors.New("response has no cache_capabilities")
	}
	for _, f := range cache.GetDigestFunctions() {
		if f == repb.DigestFunction_SHA256 {
			return nil
		}
	}
	return fmt.Errorf("SHA256 not advertised; got %v", cache.GetDigestFunctions())
}}

var casOp = operation{"CAS", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	data := randomBytes(rng, rng.Intn(32*1024)+1)
	d := digest(data)
	missing, err := c.cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{d}})
	if err != nil {
		return fmt.Errorf("FindMissingBlobs: %w", err)
	}
	if len(missing.GetMissingBlobDigests()) != 1 {
		return fmt.Errorf("FindMissingBlobs reported %d missing before upload, want 1", len(missing.GetMissingBlobDigests()))
	}
	if _, err := upload(ctx, c.cas, data); err != nil {
		return err
	}
	missing, err = c.cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{d}})
	if err != nil {
		return fmt.Errorf("FindMissingBlobs after upload: %w", err)
	}
	if len(missing.GetMissingBlobDigests()) != 0 {
		return errors.New("FindMissingBlobs still reports the blob missing after upload")
	}
	read, err := c.cas.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{d}})
	if err != nil {
		return fmt.Errorf("BatchReadBlobs: %w", err)
	}
	if len(read.Responses) != 1 {
		return fmt.Errorf("BatchReadBlobs returned %d responses, want 1", len(read.Responses))
	}
	if code := codes.Code(read.Responses[0].Status.GetCode()); code != codes.OK {
		return fmt.Errorf("BatchReadBlobs status = %s", code)
	}
	if !bytes.Equal(read.Responses[0].Data, data) {
		return fmt.Errorf("BatchReadBlobs returned %d bytes, want %d", len(read.Responses[0].Data), len(data))
	}
	return nil
}}

// batchMixedOp mixes present and absent digests in one batch and checks the
// per-entry status codes, which a single-blob round trip never exercises.
var batchMixedOp = operation{"BatchMixed", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	present := randomBytes(rng, rng.Intn(4*1024)+1)
	absent := randomBytes(rng, rng.Intn(4*1024)+1)
	if _, err := upload(ctx, c.cas, present); err != nil {
		return err
	}
	presentDigest, absentDigest := digest(present), digest(absent)
	read, err := c.cas.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests: []*repb.Digest{presentDigest, absentDigest},
	})
	if err != nil {
		return fmt.Errorf("BatchReadBlobs: %w", err)
	}
	if len(read.Responses) != 2 {
		return fmt.Errorf("BatchReadBlobs returned %d responses, want 2", len(read.Responses))
	}
	byHash := map[string]*repb.BatchReadBlobsResponse_Response{}
	for _, r := range read.Responses {
		byHash[r.Digest.GetHash()] = r
	}
	got, ok := byHash[presentDigest.Hash]
	if !ok {
		return errors.New("BatchReadBlobs omitted the present blob")
	}
	if code := codes.Code(got.Status.GetCode()); code != codes.OK {
		return fmt.Errorf("present blob status = %s, want OK", code)
	}
	if !bytes.Equal(got.Data, present) {
		return errors.New("present blob data differed")
	}
	gone, ok := byHash[absentDigest.Hash]
	if !ok {
		return errors.New("BatchReadBlobs omitted the absent blob")
	}
	if code := codes.Code(gone.Status.GetCode()); code != codes.NotFound {
		return fmt.Errorf("absent blob status = %s, want NotFound", code)
	}
	return nil
}}

// emptyBlobOp covers the REAPI rule that the empty blob is always present and
// need never be uploaded.
var emptyBlobOp = operation{"EmptyBlob", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	empty := digest(nil)
	missing, err := c.cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{empty}})
	if err != nil {
		return fmt.Errorf("FindMissingBlobs: %w", err)
	}
	if len(missing.GetMissingBlobDigests()) != 0 {
		return errors.New("the empty blob is reported missing; it must always be present")
	}
	stream, err := c.bs.Read(ctx, &bspb.ReadRequest{ResourceName: readResource(empty)})
	if err != nil {
		return err
	}
	got, err := readAll(stream)
	if err != nil {
		return fmt.Errorf("Read empty blob: %w", err)
	}
	if len(got) != 0 {
		return fmt.Errorf("empty blob read returned %d bytes", len(got))
	}
	return nil
}}

var byteStreamOp = operation{"ByteStreamChunked", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	data := randomBytes(rng, rng.Intn(128*1024)+1)
	if err := writeChunked(ctx, c.bs, rng, data); err != nil {
		return err
	}
	stream, err := c.bs.Read(ctx, &bspb.ReadRequest{ResourceName: readResource(digest(data))})
	if err != nil {
		return err
	}
	got, err := readAll(stream)
	if err != nil {
		return fmt.Errorf("Read: %w", err)
	}
	if !bytes.Equal(got, data) {
		return fmt.Errorf("round trip differed: got %d bytes, want %d", len(got), len(data))
	}
	return nil
}}

// partialReadOp exercises read_offset and read_limit, which a full-blob read
// never touches.
var partialReadOp = operation{"ByteStreamPartialRead", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	data := randomBytes(rng, rng.Intn(64*1024)+1024)
	if _, err := upload(ctx, c.cas, data); err != nil {
		return err
	}
	resource := readResource(digest(data))
	offset := rng.Intn(len(data))
	limit := rng.Intn(len(data)-offset) + 1
	stream, err := c.bs.Read(ctx, &bspb.ReadRequest{
		ResourceName: resource,
		ReadOffset:   int64(offset),
		ReadLimit:    int64(limit),
	})
	if err != nil {
		return err
	}
	got, err := readAll(stream)
	if err != nil {
		return fmt.Errorf("Read(offset=%d, limit=%d): %w", offset, limit, err)
	}
	if want := data[offset : offset+limit]; !bytes.Equal(got, want) {
		return fmt.Errorf("Read(offset=%d, limit=%d) returned %d bytes, want %d", offset, limit, len(got), len(want))
	}
	// A read with an offset but no limit must return the whole tail.
	stream, err = c.bs.Read(ctx, &bspb.ReadRequest{ResourceName: resource, ReadOffset: int64(offset)})
	if err != nil {
		return err
	}
	got, err = readAll(stream)
	if err != nil {
		return fmt.Errorf("Read(offset=%d): %w", offset, err)
	}
	if want := data[offset:]; !bytes.Equal(got, want) {
		return fmt.Errorf("Read(offset=%d) returned %d bytes, want %d", offset, len(got), len(want))
	}
	return nil
}}

// resumeOp interrupts a write partway, asks QueryWriteStatus where the server
// thinks it is, and finishes the upload from there.
var resumeOp = operation{"ByteStreamResume", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	data := randomBytes(rng, rng.Intn(64*1024)+2048)
	d := digest(data)
	resource := uploadResource(rng, d)
	split := rng.Intn(len(data)-1) + 1

	stream, err := c.bs.Write(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&bspb.WriteRequest{ResourceName: resource, Data: data[:split]}); err != nil {
		return fmt.Errorf("partial Send: %w", err)
	}
	// Abandon the stream without FinishWrite.
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("CloseSend: %w", err)
	}

	// BuildBuddy v2.290.0 answers QueryWriteStatus with Unimplemented. The
	// ByteStream contract allows that, and the documented client fallback is to
	// restart the upload from offset 0, so accept it and re-send everything.
	var committed int64
	stat, err := c.bs.QueryWriteStatus(ctx, &bspb.QueryWriteStatusRequest{ResourceName: resource})
	switch {
	case status.Code(err) == codes.Unimplemented:
		committed = 0
	case err != nil:
		return fmt.Errorf("QueryWriteStatus: %w", err)
	default:
		if stat.Complete {
			return errors.New("QueryWriteStatus reports complete for an unfinished write")
		}
		committed = stat.CommittedSize
		if committed < 0 || committed > int64(len(data)) {
			return fmt.Errorf("QueryWriteStatus committed_size = %d, out of range for %d bytes", committed, len(data))
		}
	}

	resumed, err := c.bs.Write(ctx)
	if err != nil {
		return err
	}
	if err := resumed.Send(&bspb.WriteRequest{
		ResourceName: resource,
		WriteOffset:  committed,
		Data:         data[committed:],
		FinishWrite:  true,
	}); err != nil {
		return fmt.Errorf("resumed Send from %d: %w", committed, err)
	}
	resp, err := resumed.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("resumed CloseAndRecv from %d: %w", committed, err)
	}
	if resp.CommittedSize != int64(len(data)) {
		return fmt.Errorf("resumed committed_size = %d, want %d", resp.CommittedSize, len(data))
	}
	read, err := c.bs.Read(ctx, &bspb.ReadRequest{ResourceName: readResource(d)})
	if err != nil {
		return err
	}
	got, err := readAll(read)
	if err != nil {
		return fmt.Errorf("Read after resume: %w", err)
	}
	if !bytes.Equal(got, data) {
		return fmt.Errorf("resumed blob differed: got %d bytes, want %d", len(got), len(data))
	}
	return nil
}}

// actionCacheOp stores a fully populated ActionResult, including outputs whose
// contents live in the CAS, and compares the whole proto on the way back.
var actionCacheOp = operation{"ActionCache", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	stdout := randomBytes(rng, rng.Intn(4*1024)+1)
	outputContents := randomBytes(rng, rng.Intn(8*1024)+1)
	commandBytes, err := proto.Marshal(&repb.Command{Arguments: []string{"echo", fmt.Sprint(rng.Uint64())}})
	if err != nil {
		return err
	}
	rootBytes, err := proto.Marshal(&repb.Directory{})
	if err != nil {
		return err
	}
	digests, err := upload(ctx, c.cas, commandBytes, rootBytes, stdout, outputContents)
	if err != nil {
		return err
	}
	commandDigest, rootDigest, stdoutDigest, outputDigest := digests[0], digests[1], digests[2], digests[3]
	actionBytes, err := proto.Marshal(&repb.Action{CommandDigest: commandDigest, InputRootDigest: rootDigest})
	if err != nil {
		return err
	}
	actionDigests, err := upload(ctx, c.cas, actionBytes)
	if err != nil {
		return err
	}
	actionDigest := actionDigests[0]

	want := &repb.ActionResult{
		ExitCode:     int32(rng.Intn(256)),
		StdoutDigest: stdoutDigest,
		OutputFiles: []*repb.OutputFile{{
			Path:         fmt.Sprintf("out/%d.txt", rng.Intn(1000)),
			Digest:       outputDigest,
			IsExecutable: rng.Intn(2) == 0,
		}},
		ExecutionMetadata: &repb.ExecutedActionMetadata{Worker: "reapi-test"},
	}
	if _, err := c.ac.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
		ActionDigest: actionDigest,
		ActionResult: want,
	}); err != nil {
		return fmt.Errorf("UpdateActionResult: %w", err)
	}
	got, err := c.ac.GetActionResult(ctx, &repb.GetActionResultRequest{ActionDigest: actionDigest})
	if err != nil {
		return fmt.Errorf("GetActionResult: %w", err)
	}
	if got.ExitCode != want.ExitCode {
		return fmt.Errorf("exit_code = %d, want %d", got.ExitCode, want.ExitCode)
	}
	if len(got.OutputFiles) != 1 {
		return fmt.Errorf("got %d output files, want 1", len(got.OutputFiles))
	}
	if got.OutputFiles[0].Path != want.OutputFiles[0].Path {
		return fmt.Errorf("output path = %q, want %q", got.OutputFiles[0].Path, want.OutputFiles[0].Path)
	}
	if !proto.Equal(got.OutputFiles[0].Digest, outputDigest) {
		return fmt.Errorf("output digest = %v, want %v", got.OutputFiles[0].Digest, outputDigest)
	}
	if !proto.Equal(got.StdoutDigest, stdoutDigest) {
		return fmt.Errorf("stdout digest = %v, want %v", got.StdoutDigest, stdoutDigest)
	}
	return nil
}}

// actionCacheMissOp checks that an action nobody has ever cached reports
// NotFound rather than an empty result.
var actionCacheMissOp = operation{"ActionCacheMiss", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	unknown := digest(randomBytes(rng, 64))
	_, err := c.ac.GetActionResult(ctx, &repb.GetActionResultRequest{ActionDigest: unknown})
	if code := status.Code(err); code != codes.NotFound {
		return fmt.Errorf("GetActionResult on an uncached action = %v (code %s), want NotFound", err, code)
	}
	return nil
}}

// getTreeOp builds a nested directory structure and walks it back through
// GetTree, which the flat round trips never reach.
var getTreeOp = operation{"GetTree", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	fileContents := randomBytes(rng, rng.Intn(1024)+1)
	fileDigests, err := upload(ctx, c.cas, fileContents)
	if err != nil {
		return err
	}
	leaf := &repb.Directory{
		Files: []*repb.FileNode{{Name: "leaf.txt", Digest: fileDigests[0]}},
	}
	leafBytes, err := proto.Marshal(leaf)
	if err != nil {
		return err
	}
	leafDigests, err := upload(ctx, c.cas, leafBytes)
	if err != nil {
		return err
	}
	root := &repb.Directory{
		Directories: []*repb.DirectoryNode{{Name: "sub", Digest: leafDigests[0]}},
	}
	rootBytes, err := proto.Marshal(root)
	if err != nil {
		return err
	}
	rootDigests, err := upload(ctx, c.cas, rootBytes)
	if err != nil {
		return err
	}
	stream, err := c.cas.GetTree(ctx, &repb.GetTreeRequest{RootDigest: rootDigests[0]})
	if err != nil {
		return err
	}
	var dirs []*repb.Directory
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("GetTree: %w", err)
		}
		dirs = append(dirs, resp.Directories...)
	}
	if len(dirs) != 2 {
		return fmt.Errorf("GetTree returned %d directories, want 2", len(dirs))
	}
	var sawRoot, sawLeaf bool
	for _, d := range dirs {
		if proto.Equal(d, root) {
			sawRoot = true
		}
		if proto.Equal(d, leaf) {
			sawLeaf = true
		}
	}
	if !sawRoot || !sawLeaf {
		return fmt.Errorf("GetTree missing directories (root=%v leaf=%v)", sawRoot, sawLeaf)
	}
	return nil
}}

// missingBlobReadOp asserts a read of a blob that was never written fails with
// NotFound instead of hanging or returning empty data.
var missingBlobReadOp = operation{"MissingBlobRead", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	absent := digest(randomBytes(rng, rng.Intn(1024)+1))
	stream, err := c.bs.Read(ctx, &bspb.ReadRequest{ResourceName: readResource(absent)})
	if err != nil {
		return err
	}
	got, err := readAll(stream)
	if code := status.Code(err); code != codes.NotFound {
		return fmt.Errorf("Read of an absent blob = %v (code %s) after %d bytes, want NotFound", err, code, len(got))
	}
	return nil
}}

// digestMismatchOp uploads data under the wrong digest, which the server must
// reject rather than store under a hash that does not describe it.
var digestMismatchOp = operation{"DigestMismatch", func(ctx context.Context, rng *rand.Rand, c *clients) error {
	data := randomBytes(rng, rng.Intn(4*1024)+1)
	wrong := digest(randomBytes(rng, 32))
	wrong.SizeBytes = int64(len(data))
	resp, err := c.cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: wrong, Data: data}},
	})
	if err != nil {
		// A top-level rejection is an acceptable way to refuse the write.
		if status.Code(err) == codes.InvalidArgument {
			return nil
		}
		return fmt.Errorf("BatchUpdateBlobs with a mismatched digest = %v, want InvalidArgument", err)
	}
	if len(resp.Responses) != 1 {
		return fmt.Errorf("BatchUpdateBlobs returned %d responses, want 1", len(resp.Responses))
	}
	if code := codes.Code(resp.Responses[0].Status.GetCode()); code == codes.OK {
		return errors.New("BatchUpdateBlobs accepted data that does not match its digest")
	}
	return nil
}}

var operations = []operation{
	capabilitiesOp,
	casOp,
	batchMixedOp,
	emptyBlobOp,
	byteStreamOp,
	partialReadOp,
	resumeOp,
	actionCacheOp,
	actionCacheMissOp,
	getTreeOp,
	missingBlobReadOp,
	digestMismatchOp,
}

func TestRandomREAPICacheTraffic(t *testing.T) {
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

	port, exited := startBuildBuddy(t)
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	awaitReady(t, conn, exited)

	c := &clients{
		cas:  repb.NewContentAddressableStorageClient(conn),
		ac:   repb.NewActionCacheClient(conn),
		bs:   bspb.NewByteStreamClient(conn),
		caps: repb.NewCapabilitiesClient(conn),
	}

	counts := make(map[string]int, len(operations))
	for i := 0; i < iterations; i++ {
		op := operations[rng.Intn(len(operations))]
		counts[op.name]++
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		err := op.run(ctx, rng, c)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d, %s: %v", i, op.name, err)
		}
	}
	t.Logf("completed %d operations: %v", iterations, counts)
}
