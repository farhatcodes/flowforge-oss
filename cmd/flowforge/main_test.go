package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlan(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"plan", "../../examples/order.json"}, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Join(strings.Fields(out.String()), "") != `["validate","invoice","reserve","notify"]` {
		t.Fatal(out.String())
	}
	for _, input := range []string{`{}`, `{"task":[]}`, `{"tasks":[{"id":"a","depends_on":["a"]}]}`, `{"tasks":[{"id":"a"}]} {}`} {
		path := filepath.Join(t.TempDir(), "workflow.json")
		if err := os.WriteFile(path, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		if err := run(context.Background(), []string{"plan", path}, &out); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
}
