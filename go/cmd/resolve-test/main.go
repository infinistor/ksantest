package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var nonAlphanumeric = regexp.MustCompile(`[^A-Za-z0-9]+`)

func normalized(name string) string {
	name = strings.ToLower(nonAlphanumeric.ReplaceAllString(strings.TrimSpace(name), ""))
	return strings.TrimPrefix(name, "test")
}

func availableTargets() (map[string][]string, error) {
	paths, err := filepath.Glob("*_test.go")
	if err != nil {
		return nil, err
	}
	targets := make(map[string][]string)
	for _, path := range paths {
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !strings.HasPrefix(fn.Name.Name, "Test") || fn.Body == nil {
				continue
			}
			targets[normalized(fn.Name.Name)] = append(targets[normalized(fn.Name.Name)], fn.Name.Name)
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
					name, err := strconv.Unquote(literal.Value)
					if err == nil && strings.HasPrefix(name, "test_") {
						addTarget(targets, normalized(name), fn.Name.Name+"/"+name)
					}
				}
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				literal, literalOK := call.Args[0].(*ast.BasicLit)
				if !ok || selector.Sel.Name != "Run" || !literalOK || literal.Kind != token.STRING {
					return true
				}
				name, err := strconv.Unquote(literal.Value)
				if err == nil {
					target := fn.Name.Name + "/" + name
					addTarget(targets, normalized(name), target)
				}
				return true
			})
		}
	}
	return targets, nil
}

func addTarget(targets map[string][]string, key, target string) {
	for _, existing := range targets[key] {
		if existing == target {
			return
		}
	}
	targets[key] = append(targets[key], target)
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: resolve-test <test-class> <test-method>")
		os.Exit(2)
	}
	targets, err := availableTargets()
	if err != nil {
		fmt.Fprintf(os.Stderr, "read Go tests: %v\n", err)
		os.Exit(1)
	}
	matches := append([]string(nil), targets[normalized(os.Args[2])]...)
	if len(matches) > 1 {
		className := normalized(os.Args[1])
		filtered := matches[:0]
		for _, target := range matches {
			top := strings.SplitN(target, "/", 2)[0]
			topName := normalized(top)
			if topName == className {
				filtered = append(filtered, target)
			}
		}
		matches = filtered
	}
	if len(matches) > 1 {
		subtests := make([]string, 0, len(matches))
		for _, target := range matches {
			if strings.Contains(target, "/") {
				subtests = append(subtests, target)
			}
		}
		if len(subtests) == 1 {
			matches = subtests
		}
	}
	sort.Strings(matches)
	if len(matches) != 1 {
		fmt.Fprintf(os.Stderr, "Go test not found for class=%q method=%q\n", os.Args[1], os.Args[2])
		os.Exit(1)
	}
	fmt.Println(matches[0])
}
