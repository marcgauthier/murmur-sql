// Package native projects the SQL shell onto the embedded MURMUR-GO engine.
package native

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/keyprovider"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

func key32(s string) ([32]byte, error) {
	var k [32]byte
	b, e := config.ParseHex(s, 32)
	copy(k[:], b)
	clear(b)
	return k, e
}

// resolveSecret loads a 32-byte secret from exactly one source: inline
// hex, a key file (raw 32 bytes or hex text), or a key provider URI.
// Config validation enforces the source rules; the helper fails closed
// on any other combination. Provider resolution uses a background
// context; exec helper URIs carry their own timeout.
func resolveSecret(hex, file, uri *string) ([32]byte, error) {
	var zero [32]byte
	set := 0
	for _, s := range []*string{hex, file, uri} {
		if s != nil {
			set++
		}
	}
	if set != 1 {
		return zero, fmt.Errorf("set exactly one key source")
	}
	switch {
	case hex != nil:
		return key32(*hex)
	case file != nil:
		raw, e := os.ReadFile(*file)
		if e != nil {
			return zero, e
		}
		defer clear(raw)
		return keyprovider.ParseKeyMaterial(raw)
	default:
		return keyprovider.ResolveURI(context.Background(), *uri)
	}
}

// hexRef adapts a required hex string to the *string source form; empty
// means unset so validation errors surface instead of empty keys.
func hexRef(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
func nodeID(s string) ([16]byte, error) {
	var n [16]byte
	b, e := config.ParseHex(s, 16)
	copy(n[:], b)
	return n, e
}
func integer(n uint64) (int, error) {
	if n > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("configuration value overflows native integer")
	}
	return int(n), nil
}
func BuildOptions(c config.Config, mode murmur.OpenMode) (murmur.Options, error) {
	o := murmur.DefaultOptions()
	o.Mode = mode
	if e := c.Validate(); e != nil {
		return o, e
	}
	e := c.Engine
	o.CaptureInterval = time.Duration(e.CaptureInterval)
	o.CompressionLevel = e.CompressionLevel
	var err error
	o.CaptureBytes, err = integer(uint64(e.CaptureBytes))
	if err != nil {
		return o, err
	}
	o.QueueBytes, err = integer(uint64(e.QueueBytes))
	if err != nil {
		return o, err
	}
	o.TransactionBytes, err = integer(uint64(e.TransactionBytes))
	if err != nil {
		return o, err
	}
	o.SegmentBytes, err = integer(uint64(e.SegmentBytes))
	if err != nil {
		return o, err
	}
	o.SpoolBlockBytes, err = integer(uint64(e.SpoolBlockBytes))
	if err != nil {
		return o, err
	}
	o.SpoolCacheBytes, err = integer(uint64(e.SpoolCacheBytes))
	if err != nil {
		return o, err
	}
	o.SnapshotDeltas, err = integer(uint64(e.SnapshotDeltas))
	if err != nil {
		return o, err
	}
	o.SnapshotBytes, err = integer(uint64(e.SnapshotBytes))
	if err != nil {
		return o, err
	}
	o.StorageWorkers, err = integer(uint64(e.StorageWorkers))
	if err != nil {
		return o, err
	}
	o.Scheduler.LocalShare = e.Scheduler.LocalShare
	o.Scheduler.RemoteShare = e.Scheduler.RemoteShare
	o.Scheduler.MaxDebt = time.Duration(e.Scheduler.MaxDebt)
	o.Scheduler.MaintenanceReserveEvery = e.Scheduler.MaintenanceReserveEvery
	if e.DataKeyMaxAge != nil {
		v := time.Duration(*e.DataKeyMaxAge)
		o.DataKeyMaxAge = &v
	}
	if c.Storage.DatabaseID != nil {
		v, e := nodeID(*c.Storage.DatabaseID)
		if e != nil {
			return o, e
		}
		o.DatabaseID = &v
	}
	if c.Storage.NodeID != nil {
		v, e := nodeID(*c.Storage.NodeID)
		if e != nil {
			return o, e
		}
		o.NodeID = &v
	}
	if c.Replication.Enabled {
		o.Replication, err = replicationConfig(c.Replication)
		if err != nil {
			return o, err
		}
	}
	return o, o.Validate()
}
func publicKeys(values map[string]string) (map[replication.NodeId][32]byte, error) {
	out := map[replication.NodeId][32]byte{}
	for n, k := range values {
		id, e := nodeID(n)
		if e != nil {
			return nil, e
		}
		key, e := key32(k)
		if e != nil {
			return nil, e
		}
		out[id] = key
	}
	return out, nil
}
func identities(values []string) (map[replication.NodeId]bool, error) {
	out := map[replication.NodeId]bool{}
	for _, n := range values {
		id, e := nodeID(n)
		if e != nil {
			return nil, e
		}
		out[id] = true
	}
	return out, nil
}
func certs(path string) ([][]byte, error) {
	raw, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	var out [][]byte
	for {
		block, rest := pem.Decode(raw)
		if block == nil {
			break
		}
		raw = rest
		if block.Type == "CERTIFICATE" {
			out = append(out, block.Bytes)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no certificates in %s", path)
	}
	return out, nil
}
func replicationConfig(r config.ReplicationConfig) (*replication.Config, error) {
	bind, e := netip.ParseAddrPort(r.Bind)
	if e != nil {
		return nil, e
	}
	pair, e := tls.LoadX509KeyPair(*r.CertChainFile, *r.PrivateKeyFile)
	if e != nil {
		return nil, e
	}
	roots, e := certs(*r.CaFile)
	if e != nil {
		return nil, e
	}
	private, e := os.ReadFile(*r.PrivateKeyFile)
	if e != nil {
		return nil, e
	}
	defer clear(private)
	block, _ := pem.Decode(private)
	if block == nil {
		return nil, fmt.Errorf("no replication private key")
	}
	signing, e := resolveSecret(r.SigningKeyHex, r.SigningKeyFile, r.SigningKeyURI)
	if e != nil {
		return nil, e
	}
	out := replication.NewConfig(bind, replication.Credentials{Chain: pair.Certificate, PrivateKey: append([]byte{}, block.Bytes...), Roots: roots}, signing)
	clear(signing[:])
	resolveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if r.Advertise != nil {
		v, e := resolveSocket(resolveCtx, *r.Advertise)
		if e != nil {
			return nil, e
		}
		out.Advertise = &v
	}
	for _, s := range r.Bootstrap {
		v, e := resolveSocket(resolveCtx, s)
		if e != nil {
			return nil, e
		}
		out.Bootstrap = append(out.Bootstrap, v)
		out.BootstrapHosts = append(out.BootstrapHosts, seedHostname(s))
	}
	out.TrustedOrigins, e = publicKeys(r.TrustedOrigins)
	if e != nil {
		return nil, e
	}
	out.AllowedPeers, e = identities(r.AllowedPeers)
	if e != nil {
		return nil, e
	}
	out.TrustedSnapshotSources, e = identities(r.TrustedSnapshotSources)
	if e != nil {
		return nil, e
	}
	for _, s := range r.AllowedNetworks {
		v, e := netip.ParsePrefix(s)
		if e != nil {
			return nil, e
		}
		out.AllowedNetworks = append(out.AllowedNetworks, v)
	}
	if r.SchemaMaster != nil {
		v, e := nodeID(*r.SchemaMaster)
		if e != nil {
			return nil, e
		}
		out.SchemaMaster = &v
	}
	out.Membership = r.Membership
	out.AcceptRemoteSchema = r.AcceptRemoteSchema
	out.Master = r.Master
	out.AdaptiveSync = r.AdaptiveSync
	if strings.EqualFold(r.Dissemination, "plumtree") {
		out.Dissemination = replication.DisseminationPlumtree
	}
	out.Fanout, e = integer(uint64(r.Fanout))
	if e != nil {
		return nil, e
	}
	out.Repairs, e = integer(uint64(r.Repairs))
	if e != nil {
		return nil, e
	}
	out.Sessions, e = integer(uint64(r.Sessions))
	if e != nil {
		return nil, e
	}
	out.Connections, e = integer(uint64(r.Connections))
	if e != nil {
		return nil, e
	}
	out.QueueBytes, e = integer(uint64(r.QueueBytes))
	if e != nil {
		return nil, e
	}
	out.PeerQueueBytes, e = integer(uint64(r.PeerQueueBytes))
	if e != nil {
		return nil, e
	}
	out.StagingBytes, e = integer(uint64(r.StagingBytes))
	if e != nil {
		return nil, e
	}
	out.StagingTransfers, e = integer(uint64(r.StagingTransfers))
	if e != nil {
		return nil, e
	}
	out.MaxTransactionBytes, e = integer(uint64(r.MaxTransactionBytes))
	if e != nil {
		return nil, e
	}
	out.SnapshotBytes, e = integer(uint64(r.SnapshotBytes))
	if e != nil {
		return nil, e
	}
	out.SnapshotStagingBytes, e = integer(uint64(r.SnapshotStagingBytes))
	if e != nil {
		return nil, e
	}
	out.BatchTransactions, e = integer(uint64(r.BatchTransactions))
	if e != nil {
		return nil, e
	}
	out.BatchBytes, e = integer(uint64(r.BatchBytes))
	if e != nil {
		return nil, e
	}
	out.SnapshotTransferLifetime = time.Duration(r.SnapshotTransferLifetime)
	out.IncomingInterval = time.Duration(r.IncomingInterval)
	out.AntiEntropyInterval = time.Duration(r.AntiEntropyInterval)
	out.Retention = time.Duration(r.Retention)
	out.RetentionInterval = time.Duration(r.RetentionInterval)
	out.MaximumOffline = time.Duration(r.MaximumOffline)
	out.MinimumRetained = r.MinimumRetained
	out.MergePolicies = map[replication.PolicyColumn]replication.Policy{}
	for path, policy := range r.MergePolicies {
		separator := strings.LastIndexByte(path, '.')
		table, column := path[:separator], path[separator+1:]
		var p replication.Policy
		switch strings.ToLower(policy) {
		case "counter":
			p = replication.PolicyCounter
		case "set":
			p = replication.PolicySet
		case "min":
			p = replication.PolicyMin
		case "max":
			p = replication.PolicyMax
		default:
			p = replication.PolicyLww
		}
		out.MergePolicies[replication.PolicyColumn{Table: table, Column: column}] = p
	}
	if f := r.Files; f != nil {
		key, e := resolveSecret(hexRef(f.KeyHex), nil, f.KeyURI)
		if e != nil {
			return nil, e
		}
		bind, e := netip.ParseAddrPort(f.FetchBind)
		if e != nil {
			return nil, e
		}
		v := replication.NewFilesConfig(f.Directory, key, bind)
		v.Generation = f.Generation
		v.MaxFileBytes = uint64(f.MaxFileBytes)
		v.MaxStagingBytes = uint64(f.MaxStagingBytes)
		v.ConcurrentFetches, e = integer(f.ConcurrentFetches)
		if e != nil {
			return nil, e
		}
		v.FetchInterval = time.Duration(f.FetchInterval)
		if f.PreviousKeyHex != nil || f.PreviousKeyURI != nil {
			k, e := resolveSecret(f.PreviousKeyHex, nil, f.PreviousKeyURI)
			if e != nil {
				return nil, e
			}
			v.PreviousKey = &k
		}
		out.Files = v
	}
	if b := r.Bridge; b != nil {
		v := &replication.BridgeConfig{Directory: b.Directory, Stream: b.Stream}
		v.Domain, e = nodeID(b.Domain)
		if e != nil {
			return nil, e
		}
		v.Recipient, e = key32(b.Recipient)
		if e != nil {
			return nil, e
		}
		if b.Role == "high-receiver" {
			v.Role = replication.BridgeHighReceiver
		} else {
			v.Role = replication.BridgeLowExporter
		}
		v.TrustedDomains, e = identities(b.TrustedDomains)
		if e != nil {
			return nil, e
		}
		v.TrustedOrigins, e = publicKeys(b.TrustedOrigins)
		if e != nil {
			return nil, e
		}
		v.TrustedSigners, e = publicKeys(b.TrustedSigners)
		if e != nil {
			return nil, e
		}
		if b.ReceiverSecretHex != nil || b.ReceiverSecretURI != nil {
			k, e := resolveSecret(b.ReceiverSecretHex, nil, b.ReceiverSecretURI)
			if e != nil {
				return nil, e
			}
			v.ReceiverSecret = &k
		}
		if b.SignerHex != nil || b.SignerURI != nil {
			k, e := resolveSecret(b.SignerHex, nil, b.SignerURI)
			if e != nil {
				return nil, e
			}
			v.Signer = &k
		}
		v.MaxBundleBytes, e = integer(uint64(b.MaxBundleBytes))
		if e != nil {
			return nil, e
		}
		v.MaxPayloadBytes, e = integer(uint64(b.MaxPayloadBytes))
		if e != nil {
			return nil, e
		}
		v.StagingBytes, e = integer(uint64(b.StagingBytes))
		if e != nil {
			return nil, e
		}
		out.Bridge = v
	}
	return out, out.Validate()
}

// ValidateConfig maps every setting and checks engine limits without opening storage.
func (Factory) ValidateConfig(c config.Config) error {
	_, e := BuildOptions(c, murmur.OpenExisting)
	return translate(e)
}
