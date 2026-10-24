// Live scenario: a 384 KiB file crosses a Low/High bridge through a
// shared staging directory.
//
// Go port of tests/files_bridge.rs using tests-live/goharness.
// Identical behavior: a two-node Low mesh and a two-node High mesh
// (separate DBIDs, CAs, storage keys, and file object keys) move one
// file across: Low upload, Low mesh metadata + peer fetch, signed export
// of sealed chunks, High import with re-encryption under the High object
// key, High mesh metadata + peer fetch, and metadata search. Bridge
// capture/process and peer fetch run automatically in each server, so the
// test only waits on file state. Bundle-signer and recipient keys use
// the in-module replication helpers (product parity with the Rust
// ed25519-dalek/x25519-dalek derivations).
package filesbridge

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

const (
	// lowName owns runtime/files-bridge-low/, which also holds the shared
	// staging dir so artifacts are retained/cleaned with it.
	lowName = "files-bridge-low"
	// highName owns runtime/files-bridge-high/.
	highName = "files-bridge-high"
	// stream is the bridge stream (field-files).
	stream = "field-files"
	// fileName is the bridged file (shared/field-map.bin).
	fileName = "shared/field-map.bin"
	// fileBytes is the upload size (384 KiB).
	fileBytes = 384 << 10
	// appendChunk is the upload wire chunk size (server cap: 1 MiB).
	appendChunk = 256 << 10
	// downloadChunk is the download wire chunk size (server cap: 1 MiB).
	downloadChunk = 256 << 10
	// wait is the convergence deadline (30 s).
	wait = 30 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// Independent bridge domains (each harness cluster already provisions
// its own CA).
var (
	lowDBID       = bytes16(0xE0)
	highDBID      = bytes16(0xE1)
	lowNode0      = bytes16(0xE2)
	lowNode1      = bytes16(0xE3)
	highNode0     = bytes16(0xE4)
	highNode1     = bytes16(0xE5)
	lowKey        = bytes32(0xE6)
	highKey       = bytes32(0xE7)
	lowObjectKey  = bytes32(0x11)
	highObjectKey = bytes32(0x22)
	// lowSigner is the Low bundle-signer seed (ed25519; the High
	// importer pins its public key).
	lowSigner = bytes32(0x21)
	// highSecret is the High receiver secret (X25519; seals the file
	// chunks for High).
	highSecret = bytes32(0x31)
)

func bytes16(b byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = b
	}
	return out
}

func bytes32(b byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = b
	}
	return out
}

// fileMeta is one file-metadata row (MURMUR FILE NEXT columns: name,
// size, modified_ms, sha256, local).
type fileMeta struct {
	name      string
	size      uint64
	digestHex string
	local     bool
}

// payload is the deterministic bridged payload.
func payload() []byte {
	out := make([]byte, fileBytes)
	for i := range out {
		out[i] = byte((i*37 + i/251) % 253)
	}
	return out
}

func sqlQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(value) {
	case "t", "true", "1":
		return true, nil
	case "f", "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("bad boolean %q", value)
	}
}

func parseMeta(row []string, label string) (fileMeta, error) {
	var zero fileMeta
	if len(row) != 5 {
		return zero, fmt.Errorf("%s: metadata row has %d columns, want 5", label, len(row))
	}
	size, err := strconv.ParseUint(row[1], 10, 64)
	if err != nil {
		return zero, fmt.Errorf("%s: bad size %q: %w", label, row[1], err)
	}
	local, err := parseBool(row[4])
	if err != nil {
		return zero, fmt.Errorf("%s: %w", label, err)
	}
	return fileMeta{name: row[0], size: size, digestHex: row[3], local: local}, nil
}

func label(cluster *goharness.Cluster, idx int) string {
	return fmt.Sprintf("%s-%d", cluster.Name(), idx+1)
}

// fileUpload uploads data under name on node idx (BEGIN, chunked APPEND
// over the extended protocol, FINISH). Returns the published metadata.
func fileUpload(cluster *goharness.Cluster, idx int, name string, data []byte) (fileMeta, error) {
	var zero fileMeta
	l := label(cluster, idx)
	node := cluster.Node(idx)
	rows, err := node.Simple("MURMUR FILE UPLOAD BEGIN " + sqlQuote(name))
	if err != nil {
		return zero, fmt.Errorf("%s UPLOAD BEGIN %q: %w", l, name, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return zero, fmt.Errorf("%s UPLOAD BEGIN %q: no handle", l, name)
	}
	upload := rows[0][0]
	chunk := appendChunk
	if chunk < 1 {
		chunk = 1
	}
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		if _, err := node.Execute("MURMUR FILE UPLOAD APPEND $1 $2", upload, data[off:end]); err != nil {
			return zero, fmt.Errorf("%s UPLOAD APPEND %q: %w", l, name, err)
		}
	}
	rows, err = node.Simple("MURMUR FILE UPLOAD FINISH " + sqlQuote(upload))
	if err != nil {
		return zero, fmt.Errorf("%s UPLOAD FINISH %q: %w", l, name, err)
	}
	if len(rows) == 0 {
		return zero, fmt.Errorf("%s UPLOAD FINISH %q: no metadata", l, name)
	}
	return parseMeta(rows[0], l)
}

// fileDownload downloads the full verified bytes of name from node idx.
func fileDownload(cluster *goharness.Cluster, idx int, name string) ([]byte, error) {
	l := label(cluster, idx)
	node := cluster.Node(idx)
	rows, err := node.Simple("MURMUR FILE DOWNLOAD " + sqlQuote(name))
	if err != nil {
		return nil, fmt.Errorf("%s DOWNLOAD %q: %w", l, name, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, fmt.Errorf("%s DOWNLOAD %q: no handle", l, name)
	}
	handle := rows[0][0]
	var out []byte
	for {
		page, err := node.Query(fmt.Sprintf("MURMUR FILE DOWNLOAD NEXT $1 LIMIT %d", downloadChunk), handle)
		if err != nil {
			return nil, fmt.Errorf("%s DOWNLOAD NEXT %q: %w", l, name, err)
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("%s DOWNLOAD NEXT %q: empty page", l, name)
		}
		chunk, err := page[0].Bytes(2)
		if err != nil {
			return nil, fmt.Errorf("%s DOWNLOAD NEXT %q: bad chunk: %w", l, name, err)
		}
		eof, err := page[0].Bool(3)
		if err != nil {
			return nil, fmt.Errorf("%s DOWNLOAD NEXT %q: bad eof: %w", l, name, err)
		}
		out = append(out, chunk...)
		if eof {
			break
		}
	}
	if _, err := node.Simple("MURMUR FILE CLOSE " + sqlQuote(handle)); err != nil {
		return nil, fmt.Errorf("%s CLOSE download %q: %w", l, name, err)
	}
	return out, nil
}

// fileList lists metadata on node idx, optionally restricted by a LIKE
// pattern (MURMUR FILE SEARCH); hasLike=false lists everything.
func fileList(cluster *goharness.Cluster, idx int, like string, hasLike bool) ([]fileMeta, error) {
	l := label(cluster, idx)
	node := cluster.Node(idx)
	open := "MURMUR FILE LIST"
	if hasLike {
		open = "MURMUR FILE SEARCH " + sqlQuote(like)
	}
	rows, err := node.Simple(open)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", l, open, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, fmt.Errorf("%s %s: no cursor", l, open)
	}
	cursor := rows[0][0]
	var out []fileMeta
	for {
		page, err := node.Simple("MURMUR FILE NEXT " + sqlQuote(cursor) + " LIMIT 256")
		if err != nil {
			return nil, fmt.Errorf("%s FILE NEXT: %w", l, err)
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			meta, err := parseMeta(row, l)
			if err != nil {
				return nil, err
			}
			out = append(out, meta)
		}
	}
	if _, err := node.Simple("MURMUR FILE CLOSE " + sqlQuote(cursor)); err != nil {
		return nil, fmt.Errorf("%s CLOSE cursor: %w", l, err)
	}
	return out, nil
}

// fileMetadata returns metadata for exactly name on node idx (nil when
// unlisted).
func fileMetadata(cluster *goharness.Cluster, idx int, name string) (*fileMeta, error) {
	list, err := fileList(cluster, idx, "", false)
	if err != nil {
		return nil, err
	}
	for i := range list {
		if list[i].name == name {
			return &list[i], nil
		}
	}
	return nil, nil
}

// assertNodeBytes downloads the file from cluster node idx and proves
// full integrity: exact bytes plus the uploader's digest in the
// metadata.
func assertNodeBytes(cluster *goharness.Cluster, idx int, want []byte, wantDigest string) error {
	got, err := fileDownload(cluster, idx, fileName)
	if err != nil {
		return fmt.Errorf("%s-%d download failed: %w", cluster.Name(), idx+1, err)
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("%s-%d bytes differ", cluster.Name(), idx+1)
	}
	meta, err := fileMetadata(cluster, idx, fileName)
	if err != nil {
		return err
	}
	if meta == nil {
		return fmt.Errorf("%s-%d metadata missing", cluster.Name(), idx+1)
	}
	if meta.digestHex != wantDigest {
		return fmt.Errorf("%s-%d digest differs", cluster.Name(), idx+1)
	}
	return nil
}

// waitMetadata waits until cluster node idx carries metadata for the
// file with the expected digest.
func waitMetadata(cluster *goharness.Cluster, idx int, wantDigest, what string) error {
	deadline := time.Now().Add(wait)
	for {
		meta, err := fileMetadata(cluster, idx, fileName)
		if err != nil {
			return err
		}
		if meta != nil && meta.digestHex == wantDigest {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%s: metadata never arrived", what)
		}
		time.Sleep(poll)
	}
}

// waitAvailable waits until cluster node idx holds the object locally.
func waitAvailable(cluster *goharness.Cluster, idx int, what string) error {
	deadline := time.Now().Add(wait)
	for {
		meta, err := fileMetadata(cluster, idx, fileName)
		if err != nil {
			return err
		}
		if meta != nil && meta.local {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%s: object never became available", what)
		}
		time.Sleep(poll)
	}
}

// stagedBundles lists staged *.bundle files, sorted.
func stagedBundles(staging string) ([]string, error) {
	entries, err := os.ReadDir(staging)
	if err != nil {
		return nil, fmt.Errorf("read staging dir: %w", err)
	}
	var staged []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".bundle" {
			continue
		}
		staged = append(staged, filepath.Join(staging, e.Name()))
	}
	sort.Strings(staged)
	return staged, nil
}

func TestTwoLowTwoHighFileBridgeLive(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	staging := filepath.Join(goharness.RuntimeDir(lowName), "staging")
	stagingStr := filepath.ToSlash(staging)
	recipient := replication.RecipientPublic(&highSecret)
	signerHex := goharness.Hex(lowSigner[:])
	fetch := 100 * time.Millisecond
	maxBytes := uint64(4 << 20)
	lowOpts := goharness.DefaultClusterOptions(lowName)
	lowOpts.NumNodes = 2
	lowOpts.DatabaseID = &lowDBID
	lowOpts.NodeIDs = [][16]byte{lowNode0, lowNode1}
	lowOpts.Key = lowKey
	lowOpts.Files = &goharness.FilesOptions{Key: lowObjectKey, FetchInterval: &fetch, MaxFileBytes: &maxBytes}
	lowOpts.Bridge = goharness.BridgeOptions{Nodes: map[int]goharness.BridgeNodeConfig{
		0: {
			Role:      "low-exporter",
			Domain:    goharness.Hex(lowDBID[:]),
			Directory: stagingStr,
			Stream:    stream,
			Recipient: goharness.Hex(recipient[:]),
			SignerHex: &signerHex,
		},
	}}
	low, err := goharness.NewCluster(lowOpts)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	// The High importer pins the Low bundle signer and trusts both Low
	// origin keys so imported file transactions verify; the seeds stay
	// inside the harness.
	lowOrigin0 := low.NodeOriginPubkey(0)
	lowOrigin1 := low.NodeOriginPubkey(1)
	signerPub := replication.PublicKey(&lowSigner)
	receiverSecretHex := goharness.Hex(highSecret[:])
	highOpts := goharness.DefaultClusterOptions(highName)
	highOpts.NumNodes = 2
	highOpts.DatabaseID = &highDBID
	highOpts.NodeIDs = [][16]byte{highNode0, highNode1}
	highOpts.Key = highKey
	highOpts.Files = &goharness.FilesOptions{Key: highObjectKey, FetchInterval: &fetch, MaxFileBytes: &maxBytes}
	highOpts.Bridge = goharness.BridgeOptions{Nodes: map[int]goharness.BridgeNodeConfig{
		0: {
			Role:           "high-receiver",
			Domain:         goharness.Hex(highDBID[:]),
			TrustedDomains: []string{goharness.Hex(lowDBID[:])},
			TrustedOrigins: map[string]string{
				goharness.Hex(lowNode0[:]): goharness.Hex(lowOrigin0[:]),
				goharness.Hex(lowNode1[:]): goharness.Hex(lowOrigin1[:]),
			},
			Directory:         stagingStr,
			Stream:            stream,
			Recipient:         goharness.Hex(recipient[:]),
			ReceiverSecretHex: &receiverSecretHex,
			TrustedSigners: map[string]string{
				goharness.Hex(lowNode0[:]): goharness.Hex(signerPub[:]),
			},
		},
	}}
	high, err := goharness.NewCluster(highOpts)
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}

	data := payload()
	result := runBody(low, high, data)
	failed := result != nil
	if result != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", result)
	}
	if failed {
		low.DumpForensics("files-bridge-low")
		high.DumpForensics("files-bridge-high")
	}
	low.Finish(failed)
	high.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}

func runBody(low, high *goharness.Cluster, data []byte) error {
	staging := filepath.Join(goharness.RuntimeDir(lowName), "staging")
	// Low upload, then Low mesh metadata + peer fetch.
	up, err := fileUpload(low, 0, fileName, data)
	if err != nil {
		return fmt.Errorf("low upload: %w", err)
	}
	if up.size != uint64(len(data)) {
		return fmt.Errorf("upload byte count mismatch")
	}
	if err := waitMetadata(low, 1, up.digestHex, "low mesh metadata"); err != nil {
		return err
	}
	if err := waitAvailable(low, 1, "low peer fetch"); err != nil {
		return err
	}
	if err := assertNodeBytes(low, 1, data, up.digestHex); err != nil {
		return err
	}

	// Export Low-0 to staging: sealed file-chunk bundles (plus the file
	// metadata transaction bundle). The engine's background capture
	// drives this every apply tick, so poll the staging dir.
	exportDeadline := time.Now().Add(wait)
	var staged []string
	for {
		staged, err = stagedBundles(staging)
		if err != nil {
			return err
		}
		if len(staged) >= 2 {
			break
		}
		if !time.Now().Before(exportDeadline) {
			return fmt.Errorf("staged %d bundles, want file chunks plus metadata", len(staged))
		}
		time.Sleep(poll)
	}
	// Staged artifacts must carry no document plaintext: several 1 KiB
	// windows of the payload must be absent from every staged bundle.
	// Only *.bundle files are scanned: the receiver's transient
	// .file-*.part reassembly state lives in the shared dir too once
	// import starts, and it is plaintext by design.
	windows := [][]byte{data[0:1024], data[100000:101024], data[300000:301024]}
	for _, path := range staged {
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read staged %s: %w", path, err)
		}
		for i, w := range windows {
			if bytes.Contains(raw, w) {
				return fmt.Errorf("staged %s contains plaintext window %d", path, i)
			}
		}
	}

	// Import into High-0: metadata applies, chunks install, and the
	// object re-encrypts under the High object key. The engine's
	// background process drives this every apply tick.
	if err := waitAvailable(high, 0, "high-0 import"); err != nil {
		return err
	}
	if err := assertNodeBytes(high, 0, data, up.digestHex); err != nil {
		return err
	}
	if err := assertReencrypted(low, high); err != nil {
		return err
	}

	// High mesh carries metadata to High-1, which fetches bytes from High-0.
	if err := waitMetadata(high, 1, up.digestHex, "high mesh metadata"); err != nil {
		return err
	}
	if err := waitAvailable(high, 1, "high-1 peer fetch"); err != nil {
		return err
	}
	if err := assertNodeBytes(high, 1, data, up.digestHex); err != nil {
		return err
	}

	// Search across the replicated metadata (LIKE `shared/%`: the wire
	// SEARCH matches whole names).
	found, err := fileList(high, 1, "shared/%", true)
	if err != nil {
		return err
	}
	if len(found) != 1 {
		return fmt.Errorf("search found %d files, want one", len(found))
	}
	if found[0].digestHex != up.digestHex {
		return fmt.Errorf("search digest mismatch")
	}
	if found[0].size != uint64(len(data)) {
		return fmt.Errorf("search byte count mismatch")
	}

	// No delete cascade: there is no file-delete wire command, so the
	// bridged file stays.
	return nil
}

// assertReencrypted verifies High at-rest bytes differ from Low at-rest
// bytes for the same verified content (separate domain object keys).
func assertReencrypted(low, high *goharness.Cluster) error {
	lowObj, err := firstObjectFile(low.NodeInfo(0).Dir)
	if err != nil {
		return err
	}
	highObj, err := firstObjectFile(high.NodeInfo(0).Dir)
	if err != nil {
		return err
	}
	lowRaw, err := os.ReadFile(lowObj)
	if err != nil {
		return fmt.Errorf("read %s: %w", lowObj, err)
	}
	highRaw, err := os.ReadFile(highObj)
	if err != nil {
		return fmt.Errorf("read %s: %w", highObj, err)
	}
	if bytes.Equal(lowRaw, highRaw) {
		return fmt.Errorf("high object bytes identical to low object bytes; expected re-encryption")
	}
	return nil
}

// firstObjectFile returns the first *.spfo object file under a node dir.
func firstObjectFile(root string) (string, error) {
	stack := []string{root}
	var found string
	hasFound := false
	for len(stack) > 0 {
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(dir)
		if err != nil {
			return "", fmt.Errorf("walk %s: %w", dir, err)
		}
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			if entry.IsDir() {
				stack = append(stack, path)
			} else if filepath.Ext(entry.Name()) == ".spfo" && !hasFound {
				found = path
				hasFound = true
			}
		}
	}
	if !hasFound {
		return "", fmt.Errorf("no object file under %s", root)
	}
	return found, nil
}
