package native

import (
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/murmur"
)

// TestBuildOptionsRecovery pins the config.yaml recovery section:
// defaults stay disabled, explicit values map through, and invalid
// values fail before any storage opens.
func TestBuildOptionsRecovery(t *testing.T) {
	c := config.DefaultConfig()
	o, err := BuildOptions(c, murmur.OpenExisting)
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if o.Recovery == nil || o.Recovery.IsEnabled() {
		t.Fatalf("default recovery = %+v, want disabled", o.Recovery)
	}
	c.Engine.Recovery.Enabled = true
	c.Engine.Recovery.AutoBuild = false
	c.Engine.Recovery.BuildInterval = config.Duration(30 * time.Minute)
	c.Engine.Recovery.TargetRunBytes = 64 << 20
	c.Engine.Recovery.TargetBlockBytes = 64 << 10
	c.Engine.Recovery.MaxBuildMemory = 128 << 20
	c.Engine.Recovery.RetainGenerations = 3
	o, err = BuildOptions(c, murmur.OpenExisting)
	if err != nil {
		t.Fatalf("enabled: %v", err)
	}
	ro := o.Recovery
	if ro == nil || !ro.IsEnabled() || ro.AutoBuild {
		t.Fatalf("mapped = %+v", ro)
	}
	if ro.BuildInterval != 30*time.Minute || ro.TargetRunBytes != 64<<20 ||
		ro.TargetBlockBytes != 64<<10 || ro.MaxBuildMemory != 128<<20 ||
		ro.RetainGenerations != 3 {
		t.Fatalf("mapped = %+v", ro)
	}
	c.Engine.Recovery.RetainGenerations = 0
	if _, err := BuildOptions(c, murmur.OpenExisting); err == nil {
		t.Fatal("invalid retention accepted")
	}
}
