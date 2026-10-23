package replication

import (
	"net/netip"
	"testing"
)

func validTestConfig() *Config {
	c := NewConfig(
		netip.MustParseAddrPort("127.0.0.1:0"),
		Credentials{Chain: [][]byte{[]byte("leaf")}, Roots: [][]byte{[]byte("root")}},
		fill32(1),
	)
	return c
}

func TestConfigValidateAcceptsDefaults(t *testing.T) {
	if err := validTestConfig().Validate(); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
}

func TestConfigValidateRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero batch transactions", func(c *Config) { c.BatchTransactions = 0 }},
		{"huge batch transactions", func(c *Config) { c.BatchTransactions = 1025 }},
		{"zero batch bytes", func(c *Config) { c.BatchBytes = 0 }},
		{"huge batch bytes", func(c *Config) { c.BatchBytes = (64 << 20) + 1 }},
		{"tiny queue", func(c *Config) { c.QueueBytes = 1 << 20 }},
		{"huge queue", func(c *Config) { c.QueueBytes = (128 << 20) + 1 }},
		{"huge sessions", func(c *Config) { c.Sessions = 4097 }},
		{"huge connections", func(c *Config) { c.Connections = 4097 }},
		{"zero fanout", func(c *Config) { c.Fanout = 0 }},
		{"zero repairs", func(c *Config) { c.Repairs = 0 }},
		{"fanout plus repairs over sessions", func(c *Config) { c.Fanout, c.Repairs = 30, 3 }},
		{"connections under sessions plus eight", func(c *Config) { c.Connections = c.Sessions + 7 }},
		{"queue under peer queue", func(c *Config) { c.QueueBytes = c.PeerQueueBytes - 1 }},
		{"tiny peer queue", func(c *Config) { c.PeerQueueBytes = (1 << 20) - 1 }},
		{"staging under max transaction", func(c *Config) { c.StagingBytes = c.MaxTransactionBytes }},
		{"zero staging transfers", func(c *Config) { c.StagingTransfers = 0 }},
		{"zero max transaction", func(c *Config) { c.MaxTransactionBytes = 0 }},
		{"huge max transaction", func(c *Config) { c.MaxTransactionBytes = (64 << 20) + 1 }},
		{"zero incoming interval", func(c *Config) { c.IncomingInterval = 0 }},
		{"zero anti-entropy interval", func(c *Config) { c.AntiEntropyInterval = 0 }},
		{"zero retention interval", func(c *Config) { c.RetentionInterval = 0 }},
		{"zero maximum offline", func(c *Config) { c.MaximumOffline = 0 }},
		{"zero minimum retained", func(c *Config) { c.MinimumRetained = 0 }},
		{"zero snapshot lifetime", func(c *Config) { c.SnapshotTransferLifetime = 0 }},
		{"zero snapshot staging", func(c *Config) { c.SnapshotStagingBytes = 0 }},
		{"zero snapshot bytes", func(c *Config) { c.SnapshotBytes = 0 }},
		{"empty chain", func(c *Config) { c.Credentials.Chain = nil }},
		{"empty roots", func(c *Config) { c.Credentials.Roots = nil }},
		{"master without schema master", func(c *Config) { c.Master = true }},
	}
	for _, tc := range cases {
		c := validTestConfig()
		tc.mutate(c)
		if err := c.Validate(); err == nil {
			t.Fatalf("%s accepted", tc.name)
		}
	}
}

func TestConfigValidateMasterWithSchemaMaster(t *testing.T) {
	c := validTestConfig()
	c.Master = true
	id := idOf(1)
	c.SchemaMaster = &id
	if err := c.Validate(); err != nil {
		t.Fatalf("master with schema master rejected: %v", err)
	}
}
