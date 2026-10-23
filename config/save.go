package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

var saveMu sync.Mutex

// SaveTableFlags replaces the complete stable-table-ID map, preserving comments
// and formatting in YAML. A failed write leaves the original intact.
func SaveTableFlags(path string, flags map[string]bool) error {
	saveMu.Lock()
	defer saveMu.Unlock()
	return saveFlags(path, flags)
}

func SaveTableFlag(path, id string, enabled bool) error {
	saveMu.Lock()
	defer saveMu.Unlock()
	c, e := Load(path)
	if e != nil {
		return e
	}
	if c.Replication.Tables == nil {
		c.Replication.Tables = map[string]bool{}
	}
	c.Replication.Tables[id] = enabled
	return saveFlags(path, c.Replication.Tables)
}

func saveFlags(path string, flags map[string]bool) error {
	for id := range flags {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("empty table ID")
		}
	}
	old, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	info, e := os.Stat(path)
	if e != nil {
		return e
	}

	var doc yaml.Node
	if e = yaml.Unmarshal(old, &doc); e != nil {
		return e
	}

	if e = updateTablesNode(&doc, flags); e != nil {
		return e
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if e = enc.Encode(&doc); e != nil {
		return e
	}

	c, e := Parse(buf.Bytes())
	if e != nil {
		return e
	}
	if len(c.Replication.Tables) != len(flags) {
		return fmt.Errorf("table flag replacement mismatch")
	}
	for k, v := range flags {
		if got, ok := c.Replication.Tables[k]; !ok || got != v {
			return fmt.Errorf("table flag replacement mismatch")
		}
	}

	f, e := os.CreateTemp(filepath.Dir(path), ".murmur-config-*")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(info.Mode().Perm()); e == nil {
		_, e = f.Write(buf.Bytes())
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		return e
	}
	if runtime.GOOS != "windows" {
		d, e := os.Open(filepath.Dir(path))
		if e != nil {
			return e
		}
		defer d.Close()
		return d.Sync()
	}
	return nil
}

func updateTablesNode(node *yaml.Node, flags map[string]bool) error {
	root := node
	if root.Kind == yaml.DocumentNode {
		if len(root.Content) == 0 {
			root.Content = append(root.Content, &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"})
		}
		root = root.Content[0]
	}
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping node, got %v", root.Kind)
	}

	var replVal *yaml.Node
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == "replication" {
			replVal = root.Content[i+1]
			break
		}
	}
	if replVal == nil {
		replKey := &yaml.Node{Kind: yaml.ScalarNode, Value: "replication", Tag: "!!str"}
		replVal = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		root.Content = append(root.Content, replKey, replVal)
	}
	if replVal.Kind != yaml.MappingNode {
		return fmt.Errorf("expected replication mapping node")
	}

	var tablesVal *yaml.Node
	for i := 0; i < len(replVal.Content); i += 2 {
		if replVal.Content[i].Value == "tables" {
			tablesVal = replVal.Content[i+1]
			break
		}
	}
	if tablesVal == nil {
		tablesKey := &yaml.Node{Kind: yaml.ScalarNode, Value: "tables", Tag: "!!str"}
		tablesVal = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		replVal.Content = append(replVal.Content, tablesKey, tablesVal)
	}

	existingKeys := map[string]*yaml.Node{}
	for i := 0; i < len(tablesVal.Content); i += 2 {
		existingKeys[tablesVal.Content[i].Value] = tablesVal.Content[i]
	}

	keys := make([]string, 0, len(flags))
	for k := range flags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newContent := make([]*yaml.Node, 0, len(keys)*2)
	for _, k := range keys {
		valStr := "false"
		if flags[k] {
			valStr = "true"
		}
		kNode, ok := existingKeys[k]
		if !ok {
			kNode = &yaml.Node{Kind: yaml.ScalarNode, Value: k, Tag: "!!str"}
		}
		vNode := &yaml.Node{Kind: yaml.ScalarNode, Value: valStr, Tag: "!!bool"}
		newContent = append(newContent, kNode, vNode)
	}
	tablesVal.Kind = yaml.MappingNode
	tablesVal.Tag = "!!map"
	tablesVal.Content = newContent

	return nil
}
