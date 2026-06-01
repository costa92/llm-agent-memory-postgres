package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

func TestCommandPackageCompiles(t *testing.T) {}

func TestMain_UsesLLMAgentMemoryPGURL(t *testing.T) {
	t.Helper()

	path := filepath.Join(".", "main.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	file, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel == nil || sel.Sel.Name != "Getenv" {
			return true
		}
		if len(call.Args) != 1 {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Value != "\"LLM_AGENT_MEMORY_PG_URL\"" {
			return true
		}
		found = true
		return false
	})

	if !found {
		t.Fatalf("%s must read LLM_AGENT_MEMORY_PG_URL", path)
	}
}
