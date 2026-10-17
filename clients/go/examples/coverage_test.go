package examples_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Coverage is tied to the documented client feature catalog, not a hand-copied
// second list. New README feature groups must add reviewed executable examples.
func TestFeatureCoverage(t *testing.T) {
	raw, err := os.ReadFile("coverage.json")
	if err != nil {
		t.Fatal(err)
	}
	var coverage map[string][]string
	if err = json.Unmarshal(raw, &coverage); err != nil {
		t.Fatal(err)
	}
	readme, err := os.ReadFile("../README.md")
	if err != nil {
		t.Fatal(err)
	}
	sections := strings.SplitN(string(readme), "## Features\n", 2)
	if len(sections) != 2 {
		t.Fatal("client feature table missing")
	}
	catalog := strings.SplitN(sections[1], "\n## ", 2)[0]
	features := map[string]bool{}
	mapped := map[string]bool{}
	for _, line := range strings.Split(catalog, "\n") {
		if !strings.HasPrefix(line, "| ") {
			continue
		}
		parts := strings.Split(line, "|")
		name := strings.TrimSpace(parts[1])
		if name == "Feature" || strings.HasPrefix(name, "---") {
			continue
		}
		features[name] = true
		if len(coverage[name]) == 0 {
			t.Errorf("feature %q has no executable example", name)
		}
	}
	for feature, names := range coverage {
		if !features[feature] {
			t.Errorf("obsolete feature mapping %q", feature)
		}
		for _, name := range names {
			mapped[name] = true
			main := filepath.Join(name, "main.go")
			if _, err := os.Stat(main); err != nil {
				t.Errorf("example %q has no %s", name, main)
			}
		}
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "internal" {
			continue
		}
		if !mapped[entry.Name()] {
			t.Errorf("example %q is not mapped to a feature", entry.Name())
		}
	}
}
