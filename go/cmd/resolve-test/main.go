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
	"strings"
)

var nonAlphanumeric = regexp.MustCompile(`[^A-Za-z0-9]+`)

// classFiles maps Java-style class names to Go test source files.
var classFiles = map[string]string{
	"accelerate":          "accelerate_test.go",
	"access":              "access_test.go",
	"acl":                 "acl_test.go",
	"analytics":           "analytics_test.go",
	"backend":             "backend_test.go",
	"copyobject":          "copy_object_test.go",
	"cors":                "cors_test.go",
	"cse":                 "cse_test.go",
	"deletebucket":        "delete_bucket_test.go",
	"deleteobjects":       "delete_objects_test.go",
	"getobject":           "get_object_test.go",
	"getobjectattributes": "get_object_attributes_test.go",
	"grants":              "grants_test.go",
	"inventory":           "inventory_test.go",
	"kms":                 "kms_test.go",
	"lifecycle":           "lifecycle_test.go",
	"listbuckets":         "list_buckets_test.go",
	"listobjects":         "list_objects_test.go",
	"listobjectsv2":       "list_objects_v2_test.go",
	"listobjectsversions": "list_objects_versions_test.go",
	"lock":                "lock_test.go",
	"logging":             "logging_test.go",
	"metrics":             "metrics_test.go",
	"multipart":           "multipart_test.go",
	"notification":        "notification_test.go",
	"ownership":           "ownership_test.go",
	"payment":             "payment_test.go",
	"policy":              "policy_test.go",
	"post":                "post_test.go",
	"putbucket":           "put_bucket_test.go",
	"putobject":           "put_object_test.go",
	"replication":         "replication_test.go",
	"selectobjectcontent": "select_object_content_test.go",
	"ssec":                "sse_c_test.go",
	"sses3":               "sse_s3_test.go",
	"taggings":            "taggings_test.go",
	"versioning":          "versioning_test.go",
	"website":             "website_test.go",
}

func normalized(name string) string {
	name = strings.ToLower(nonAlphanumeric.ReplaceAllString(strings.TrimSpace(name), ""))
	return strings.TrimPrefix(name, "test")
}

func classKey(name string) string {
	return normalized(name)
}

func testsInFile(path string) ([]string, error) {
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Test") || fn.Body == nil {
			continue
		}
		names = append(names, fn.Name.Name)
	}
	sort.Strings(names)
	return names, nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: resolve-test <test-class> <test-method>")
		os.Exit(2)
	}
	file, ok := classFiles[classKey(os.Args[1])]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown test class %q\n", os.Args[1])
		os.Exit(1)
	}
	if _, err := os.Stat(file); err != nil {
		fmt.Fprintf(os.Stderr, "test file for class %q not found: %v\n", os.Args[1], err)
		os.Exit(1)
	}
	names, err := testsInFile(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse %s: %v\n", file, err)
		os.Exit(1)
	}
	want := normalized(os.Args[2])
	classNorm := classKey(os.Args[1])
	var matches []string
	for _, name := range names {
		n := normalized(name)
		if n == want {
			matches = append(matches, name)
			continue
		}
		// Collision-disambiguated names: TestBackend_MultipartUpload
		if strings.HasPrefix(n, classNorm) && strings.TrimPrefix(n, classNorm) == want {
			matches = append(matches, name)
		}
	}
	if len(matches) != 1 {
		fmt.Fprintf(os.Stderr, "Go test not found for class=%q method=%q in %s\n", os.Args[1], os.Args[2], filepath.Base(file))
		os.Exit(1)
	}
	fmt.Println(matches[0])
}
