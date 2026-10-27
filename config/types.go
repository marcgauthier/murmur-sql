// Package config owns config.yaml. Engine code never writes configuration files.
package config

type Config struct {
	Server      ServerConfig      `yaml:"server" json:"server"`
	Storage     StorageConfig     `yaml:"storage" json:"storage"`
	Engine      EngineConfig      `yaml:"engine" json:"engine"`
	Auth        AuthConfig        `yaml:"auth" json:"auth"`
	Replication ReplicationConfig `yaml:"replication" json:"replication"`
	Logging     LoggingConfig     `yaml:"logging" json:"logging"`
	Backups     BackupConfig      `yaml:"backups" json:"backups"`
}

type ServerConfig struct {
	Bind           string    `yaml:"bind" json:"bind"`
	Production     bool      `yaml:"production" json:"production"`
	TLS            TlsConfig `yaml:"tls" json:"tls"`
	MaxConnections uint32    `yaml:"max_connections" json:"max_connections"`
}

type TlsConfig struct {
	Enabled  bool    `yaml:"enabled" json:"enabled"`
	CertFile *string `yaml:"cert_file" json:"cert_file"`
	KeyFile  *string `yaml:"key_file" json:"key_file"`
	CaFile   *string `yaml:"ca_file" json:"ca_file"`
}

type StorageConfig struct {
	DataDir    string  `yaml:"data_dir" json:"data_dir"`
	DatabaseID *string `yaml:"database_id" json:"database_id"`
	NodeID     *string `yaml:"node_id" json:"node_id"`
}

type EngineConfig struct {
	CaptureInterval  Duration        `yaml:"capture_interval" json:"capture_interval"`
	CaptureBytes     Bytes           `yaml:"capture_bytes" json:"capture_bytes"`
	QueueBytes       Bytes           `yaml:"queue_bytes" json:"queue_bytes"`
	TransactionBytes Bytes           `yaml:"transaction_bytes" json:"transaction_bytes"`
	SegmentBytes     Bytes           `yaml:"segment_bytes" json:"segment_bytes"`
	SpoolBlockBytes  Bytes           `yaml:"spool_block_bytes" json:"spool_block_bytes"`
	SpoolCacheBytes  Bytes           `yaml:"spool_cache_bytes" json:"spool_cache_bytes"`
	SnapshotDeltas   uint64          `yaml:"snapshot_deltas" json:"snapshot_deltas"`
	SnapshotBytes    Bytes           `yaml:"snapshot_bytes" json:"snapshot_bytes"`
	CompressionLevel int             `yaml:"compression_level" json:"compression_level"`
	StorageWorkers   uint64          `yaml:"storage_workers" json:"storage_workers"`
	DataKeyMaxAge    *Duration       `yaml:"data_key_max_age" json:"data_key_max_age"`
	Scheduler        SchedulerConfig `yaml:"scheduler" json:"scheduler"`
	Recovery         RecoveryConfig  `yaml:"recovery" json:"recovery"`
}

// RecoveryConfig controls the optional disposable recovery cache:
// sorted, compressed row runs rebuilt from Spool to accelerate
// restarts. Disabled by default; enabling never changes the
// authoritative storage format.
type RecoveryConfig struct {
	Enabled           bool     `yaml:"enabled" json:"enabled"`
	AutoBuild         bool     `yaml:"auto_build" json:"auto_build"`
	BuildInterval     Duration `yaml:"build_interval" json:"build_interval"`
	TargetRunBytes    Bytes    `yaml:"target_run_bytes" json:"target_run_bytes"`
	TargetBlockBytes  Bytes    `yaml:"target_block_bytes" json:"target_block_bytes"`
	MaxBuildMemory    Bytes    `yaml:"max_build_memory" json:"max_build_memory"`
	RetainGenerations int      `yaml:"retain_generations" json:"retain_generations"`
}

type SchedulerConfig struct {
	LocalShare              uint32   `yaml:"local_share" json:"local_share"`
	RemoteShare             uint32   `yaml:"remote_share" json:"remote_share"`
	MaxDebt                 Duration `yaml:"max_debt" json:"max_debt"`
	MaintenanceReserveEvery uint64   `yaml:"maintenance_reserve_every" json:"maintenance_reserve_every"`
}

type AuthConfig struct {
	Users []UserConfig `yaml:"users" json:"users"`
}

type UserConfig struct {
	Name         string `yaml:"name" json:"name"`
	PasswordHash string `yaml:"password_hash" json:"password_hash"`
}

type ReplicationConfig struct {
	Enabled                  bool                     `yaml:"enabled" json:"enabled"`
	Bind                     string                   `yaml:"bind" json:"bind"`
	Advertise                *string                  `yaml:"advertise" json:"advertise"`
	Bootstrap                []string                 `yaml:"bootstrap" json:"bootstrap"`
	CertChainFile            *string                  `yaml:"cert_chain_file" json:"cert_chain_file"`
	PrivateKeyFile           *string                  `yaml:"private_key_file" json:"private_key_file"`
	CaFile                   *string                  `yaml:"ca_file" json:"ca_file"`
	SigningKeyHex            *string                  `yaml:"signing_key_hex" json:"signing_key_hex"`
	SigningKeyFile           *string                  `yaml:"signing_key_file" json:"signing_key_file"`
	SigningKeyURI            *string                  `yaml:"signing_key_uri" json:"signing_key_uri"`
	TrustedOrigins           map[string]string        `yaml:"trusted_origins" json:"trusted_origins"`
	AllowedPeers             []string                 `yaml:"allowed_peers" json:"allowed_peers"`
	AllowedNetworks          []string                 `yaml:"allowed_networks" json:"allowed_networks"`
	TrustedSnapshotSources   []string                 `yaml:"trusted_snapshot_sources" json:"trusted_snapshot_sources"`
	Membership               bool                     `yaml:"membership" json:"membership"`
	Dissemination            string                   `yaml:"dissemination" json:"dissemination"`
	AcceptRemoteSchema       bool                     `yaml:"accept_remote_schema" json:"accept_remote_schema"`
	Master                   bool                     `yaml:"master" json:"master"`
	SchemaMaster             *string                  `yaml:"schema_master" json:"schema_master"`
	Fanout                   uint64                   `yaml:"fanout" json:"fanout"`
	Repairs                  uint64                   `yaml:"repairs" json:"repairs"`
	Sessions                 uint64                   `yaml:"sessions" json:"sessions"`
	Connections              uint64                   `yaml:"connections" json:"connections"`
	QueueBytes               Bytes                    `yaml:"queue_bytes" json:"queue_bytes"`
	PeerQueueBytes           Bytes                    `yaml:"peer_queue_bytes" json:"peer_queue_bytes"`
	StagingBytes             Bytes                    `yaml:"staging_bytes" json:"staging_bytes"`
	StagingTransfers         uint64                   `yaml:"staging_transfers" json:"staging_transfers"`
	MaxTransactionBytes      Bytes                    `yaml:"max_transaction_bytes" json:"max_transaction_bytes"`
	SnapshotBytes            Bytes                    `yaml:"snapshot_bytes" json:"snapshot_bytes"`
	SnapshotStagingBytes     Bytes                    `yaml:"snapshot_staging_bytes" json:"snapshot_staging_bytes"`
	SnapshotTransferLifetime Duration                 `yaml:"snapshot_transfer_lifetime" json:"snapshot_transfer_lifetime"`
	IncomingInterval         Duration                 `yaml:"incoming_interval" json:"incoming_interval"`
	BatchTransactions        uint64                   `yaml:"batch_transactions" json:"batch_transactions"`
	BatchBytes               Bytes                    `yaml:"batch_bytes" json:"batch_bytes"`
	AdaptiveSync             bool                     `yaml:"adaptive_sync" json:"adaptive_sync"`
	AntiEntropyInterval      Duration                 `yaml:"anti_entropy_interval" json:"anti_entropy_interval"`
	Retention                Duration                 `yaml:"retention" json:"retention"`
	RetentionInterval        Duration                 `yaml:"retention_interval" json:"retention_interval"`
	MinimumRetained          uint64                   `yaml:"minimum_retained" json:"minimum_retained"`
	MaximumOffline           Duration                 `yaml:"maximum_offline" json:"maximum_offline"`
	MergePolicies            map[string]string        `yaml:"merge_policies" json:"merge_policies"`
	Tables                   map[string]bool          `yaml:"tables" json:"tables"`
	Files                    *ReplicationFilesConfig  `yaml:"files" json:"files"`
	Bridge                   *ReplicationBridgeConfig `yaml:"bridge" json:"bridge"`
}

type ReplicationFilesConfig struct {
	Directory         string   `yaml:"directory" json:"directory"`
	KeyHex            string   `yaml:"key_hex" json:"key_hex"`
	KeyURI            *string  `yaml:"key_uri" json:"key_uri"`
	PreviousKeyHex    *string  `yaml:"previous_key_hex" json:"previous_key_hex"`
	PreviousKeyURI    *string  `yaml:"previous_key_uri" json:"previous_key_uri"`
	Generation        uint64   `yaml:"generation" json:"generation"`
	FetchBind         string   `yaml:"fetch_bind" json:"fetch_bind"`
	MaxFileBytes      Bytes    `yaml:"max_file_bytes" json:"max_file_bytes"`
	MaxStagingBytes   Bytes    `yaml:"max_staging_bytes" json:"max_staging_bytes"`
	ConcurrentFetches uint64   `yaml:"concurrent_fetches" json:"concurrent_fetches"`
	FetchInterval     Duration `yaml:"fetch_interval" json:"fetch_interval"`
}

type ReplicationBridgeConfig struct {
	Role              string            `yaml:"role" json:"role"`
	Domain            string            `yaml:"domain" json:"domain"`
	TrustedDomains    []string          `yaml:"trusted_domains" json:"trusted_domains"`
	TrustedOrigins    map[string]string `yaml:"trusted_origins" json:"trusted_origins"`
	Directory         string            `yaml:"directory" json:"directory"`
	Stream            string            `yaml:"stream" json:"stream"`
	Recipient         string            `yaml:"recipient" json:"recipient"`
	ReceiverSecretHex *string           `yaml:"receiver_secret_hex" json:"receiver_secret_hex"`
	ReceiverSecretURI *string           `yaml:"receiver_secret_uri" json:"receiver_secret_uri"`
	SignerHex         *string           `yaml:"signer_hex" json:"signer_hex"`
	SignerURI         *string           `yaml:"signer_uri" json:"signer_uri"`
	TrustedSigners    map[string]string `yaml:"trusted_signers" json:"trusted_signers"`
	MaxBundleBytes    Bytes             `yaml:"max_bundle_bytes" json:"max_bundle_bytes"`
	MaxPayloadBytes   Bytes             `yaml:"max_payload_bytes" json:"max_payload_bytes"`
	StagingBytes      Bytes             `yaml:"staging_bytes" json:"staging_bytes"`
}

type LoggingConfig struct {
	Dir           string             `yaml:"dir" json:"dir"`
	Level         string             `yaml:"level" json:"level"`
	FilePrefix    string             `yaml:"file_prefix" json:"file_prefix"`
	Rotation      string             `yaml:"rotation" json:"rotation"`
	RetentionDays uint32             `yaml:"retention_days" json:"retention_days"`
	Console       bool               `yaml:"console" json:"console"`
	Victorialogs  VictoriaLogsConfig `yaml:"victorialogs" json:"victorialogs"`
}

type VictoriaLogsConfig struct {
	Enabled     bool              `yaml:"enabled" json:"enabled"`
	URL         string            `yaml:"url" json:"url"`
	Username    *string           `yaml:"username" json:"username"`
	Password    *string           `yaml:"password" json:"password"`
	BearerToken *string           `yaml:"bearer_token" json:"bearer_token"`
	Labels      map[string]string `yaml:"labels" json:"labels"`
}

type BackupConfig struct {
	Dir string `yaml:"dir" json:"dir"`
}

func DefaultConfig() Config {
	return Config{
		Server:      DefaultServerConfig(),
		Storage:     DefaultStorageConfig(),
		Engine:      DefaultEngineConfig(),
		Auth:        DefaultAuthConfig(),
		Replication: DefaultReplicationConfig(),
		Logging:     DefaultLoggingConfig(),
		Backups:     DefaultBackupConfig(),
	}
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Bind:           "127.0.0.1:5432",
		Production:     false,
		TLS:            DefaultTlsConfig(),
		MaxConnections: 256,
	}
}

func DefaultTlsConfig() TlsConfig {
	return TlsConfig{
		Enabled: false,
	}
}

func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		DataDir: "./data",
	}
}

func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		CaptureInterval:  Duration((1) * 1e9),
		CaptureBytes:     8 << 20,
		QueueBytes:       64 << 20,
		TransactionBytes: 64 << 20,
		SegmentBytes:     256 << 20,
		SpoolBlockBytes:  64 << 10,
		SpoolCacheBytes:  32 << 20,
		SnapshotDeltas:   128,
		SnapshotBytes:    256 << 20,
		CompressionLevel: 3,
		StorageWorkers:   2,
		Scheduler:        DefaultSchedulerConfig(),
		Recovery:         DefaultRecoveryConfig(),
	}
}

func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{
		Enabled:           false,
		AutoBuild:         true,
		BuildInterval:     Duration((3600) * 1e9),
		TargetRunBytes:    256 << 20,
		TargetBlockBytes:  256 << 10,
		MaxBuildMemory:    256 << 20,
		RetainGenerations: 2,
	}
}

func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		LocalShare:              90,
		RemoteShare:             10,
		MaxDebt:                 Duration((1) * 1e9),
		MaintenanceReserveEvery: 100,
	}
}

func DefaultAuthConfig() AuthConfig {
	return AuthConfig{}
}

func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		Tables:                   map[string]bool{},
		Enabled:                  false,
		Bind:                     "127.0.0.1:9001",
		Membership:               true,
		Dissemination:            "gossip",
		AcceptRemoteSchema:       true,
		Master:                   false,
		Fanout:                   4,
		Repairs:                  2,
		Sessions:                 32,
		Connections:              64,
		QueueBytes:               64 << 20,
		PeerQueueBytes:           4 << 20,
		StagingBytes:             256 << 20,
		StagingTransfers:         4096,
		MaxTransactionBytes:      64 << 20,
		SnapshotBytes:            512 << 20,
		SnapshotStagingBytes:     512 << 20,
		SnapshotTransferLifetime: Duration((7 * 86400) * 1e9),
		IncomingInterval:         Duration((1) * 1e9),
		BatchTransactions:        64,
		BatchBytes:               4 << 20,
		AdaptiveSync:             false,
		AntiEntropyInterval:      Duration((10) * 1e9),
		Retention:                Duration((3600) * 1e9),
		RetentionInterval:        Duration((60) * 1e9),
		MinimumRetained:          1000,
		MaximumOffline:           Duration((7 * 86400) * 1e9),
	}
}

func DefaultReplicationFilesConfig() ReplicationFilesConfig {
	return ReplicationFilesConfig{
		Directory:         "./files",
		KeyHex:            "",
		Generation:        0,
		FetchBind:         "127.0.0.1:0",
		MaxFileBytes:      4 << 30,
		MaxStagingBytes:   1 << 30,
		ConcurrentFetches: 2,
		FetchInterval:     Duration((30) * 1e9),
	}
}

func DefaultReplicationBridgeConfig() ReplicationBridgeConfig {
	return ReplicationBridgeConfig{
		Role:            "low-exporter",
		Domain:          "",
		Directory:       "./bridge",
		Stream:          "main",
		Recipient:       "",
		MaxBundleBytes:  16 << 20,
		MaxPayloadBytes: 64 << 20,
		StagingBytes:    256 << 20,
	}
}

func DefaultLoggingConfig() LoggingConfig {
	return LoggingConfig{
		Dir:           "./logs",
		Level:         "info",
		FilePrefix:    "murmur",
		Rotation:      "daily",
		RetentionDays: 14,
		Console:       false,
		Victorialogs:  DefaultVictoriaLogsConfig(),
	}
}

func DefaultVictoriaLogsConfig() VictoriaLogsConfig {
	return VictoriaLogsConfig{
		Enabled: false,
		URL:     "http://127.0.0.1:9428",
	}
}

func DefaultBackupConfig() BackupConfig {
	return BackupConfig{
		Dir: "./backups",
	}
}
