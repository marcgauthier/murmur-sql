package replication

import (
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"net/netip"
	"strings"
)

// NodeID extracts the node identity from a certificate's node URI SAN.
// The DER must parse completely with no trailing bytes.
func NodeID(der []byte) (NodeId, error) {
	var zero NodeId
	var cert certificate
	rest, err := asn1.Unmarshal(der, &cert)
	if err != nil {
		return zero, authErr("%v", err)
	}
	if len(rest) != 0 {
		return zero, authErr("trailing certificate bytes")
	}
	var found *NodeId
	for _, ext := range cert.TBSCertificate.Extensions {
		if !ext.Id.Equal(asn1.ObjectIdentifier{2, 5, 29, 17}) {
			continue
		}
		var names []asn1.RawValue
		if rest, err := asn1.Unmarshal(ext.Value, &names); err != nil || len(rest) != 0 {
			return zero, authErr("invalid subject alternative names")
		}
		for _, name := range names {
			if name.Class != 2 || name.Tag != 6 {
				continue
			}
			value, ok := strings.CutPrefix(string(name.Bytes), "replicateddb://node/")
			if !ok {
				continue
			}
			digits := strings.ReplaceAll(value, "-", "")
			if len(digits) != 32 {
				return zero, authErr("invalid node identity")
			}
			var id NodeId
			for i := range id {
				var v uint64
				for _, c := range digits[2*i : 2*i+2] {
					var d uint64
					switch {
					case c >= '0' && c <= '9':
						d = uint64(c - '0')
					case c >= 'a' && c <= 'f':
						d = uint64(c-'a') + 10
					case c >= 'A' && c <= 'F':
						d = uint64(c-'A') + 10
					default:
						return zero, authErr("invalid node identity")
					}
					v = v*16 + d
				}
				id[i] = byte(v)
			}
			if id == ([16]byte{}) || found != nil {
				return zero, authErr("ambiguous node identity")
			}
			cp := id
			found = &cp
		}
	}
	if found == nil {
		return zero, authErr("missing node URI SAN")
	}
	return *found, nil
}

type certificate struct {
	TBSCertificate tbsCertificate
	// Signature algorithm and value are verified by the TLS layer, not here.
	SignatureAlgorithm pkixAlgorithmIdentifier
	SignatureValue     asn1.BitString
}

type pkixAlgorithmIdentifier struct {
	Algorithm  asn1.ObjectIdentifier
	Parameters asn1.RawValue `asn1:"optional"`
}

type tbsCertificate struct {
	Raw           asn1.RawContent
	Version       int `asn1:"optional,explicit,default:0,tag:0"`
	SerialNumber  asn1.RawValue
	Signature     pkixAlgorithmIdentifier
	Issuer        asn1.RawValue
	Validity      asn1.RawValue
	Subject       asn1.RawValue
	PublicKey     asn1.RawValue
	IssuerUnique  asn1.BitString   `asn1:"optional,tag:1"`
	SubjectUnique asn1.BitString   `asn1:"optional,tag:2"`
	Extensions    []pkix.Extension `asn1:"optional,explicit,tag:3"`
}

// ParsePEMChain decodes a PEM certificate chain to DER blocks.
func ParsePEMChain(pemBytes []byte) ([][]byte, error) {
	var out [][]byte
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Bytes) == 0 {
			return nil, authErr("invalid PEM certificate")
		}
		out = append(out, block.Bytes)
	}
	if len(out) == 0 {
		return nil, authErr("empty certificate chain")
	}
	return out, nil
}

// NetworkAllowed reports whether a peer address passes the allowlist.
// Empty means unrestricted. IPv4-mapped IPv6 collapses to IPv4 first.
func NetworkAllowed(config *Config, addr netip.AddrPort) bool {
	if len(config.AllowedNetworks) == 0 {
		return true
	}
	ip := addr.Addr()
	if ip.Is4In6() {
		ip = netip.AddrFrom4(ip.As4())
	}
	for _, n := range config.AllowedNetworks {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
