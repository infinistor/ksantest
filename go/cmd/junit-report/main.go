package main

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type event struct {
	Time                          time.Time
	Action, Package, Test, Output string
	Elapsed                       float64
}
type result struct {
	Name, Status, Output string
	Elapsed              float64
}
type testSuites struct {
	XMLName xml.Name    `xml:"testsuites"`
	Suites  []testSuite `xml:"testsuite"`
}
type testSuite struct {
	Name     string     `xml:"name,attr"`
	Tests    int        `xml:"tests,attr"`
	Errors   int        `xml:"errors,attr"`
	Failures int        `xml:"failures,attr"`
	Skipped  int        `xml:"skipped,attr"`
	Time     string     `xml:"time,attr"`
	Cases    []testCase `xml:"testcase"`
}
type testCase struct {
	ClassName string   `xml:"classname,attr"`
	Name      string   `xml:"name,attr"`
	Time      string   `xml:"time,attr"`
	Failure   *failure `xml:"failure,omitempty"`
	Skipped   *skipped `xml:"skipped,omitempty"`
	SystemOut string   `xml:"system-out,omitempty"`
}
type failure struct {
	Message string `xml:"message,attr"`
	Output  string `xml:",chardata"`
}
type skipped struct {
	Message string `xml:"message,attr,omitempty"`
}

// fileToClass maps migration test filenames to Java-style class names.
var fileToClass = map[string]string{
	"accelerate_test.go":            "Accelerate",
	"access_test.go":                "Access",
	"acl_test.go":                   "ACL",
	"analytics_test.go":             "Analytics",
	"backend_test.go":               "Backend",
	"copy_object_test.go":           "CopyObject",
	"cors_test.go":                  "Cors",
	"cse_test.go":                   "CSE",
	"delete_bucket_test.go":         "DeleteBucket",
	"delete_objects_test.go":        "DeleteObjects",
	"get_object_test.go":            "GetObject",
	"get_object_attributes_test.go": "GetObjectAttributes",
	"grants_test.go":                "Grants",
	"inventory_test.go":             "Inventory",
	"lifecycle_test.go":             "LifeCycle",
	"list_buckets_test.go":          "ListBuckets",
	"list_objects_test.go":          "ListObjects",
	"list_objects_v2_test.go":       "ListObjectsV2",
	"list_objects_versions_test.go": "ListObjectsVersions",
	"lock_test.go":                  "Lock",
	"logging_test.go":               "Logging",
	"metrics_test.go":               "Metrics",
	"multipart_test.go":             "Multipart",
	"notification_test.go":          "Notification",
	"ownership_test.go":             "Ownership",
	"payment_test.go":               "Payment",
	"policy_test.go":                "Policy",
	"post_test.go":                  "Post",
	"put_bucket_test.go":            "PutBucket",
	"put_object_test.go":            "PutObject",
	"replication_test.go":           "Replication",
	"select_object_content_test.go": "SelectObjectContent",
	"sse_c_test.go":                 "SSE_C",
	"sse_s3_test.go":                "SSE_S3",
	"taggings_test.go":              "Taggings",
	"versioning_test.go":            "Versioning",
	"website_test.go":               "Website",
}

func main() {
	output := flag.String("output", "Result_go.xml", "JUnit XML output path")
	flag.Parse()
	testClass, err := loadMigrationTests()
	if err != nil {
		fatal(err)
	}
	results := leafResults(readResults(), testClass)
	suite := testSuite{Name: "go"}
	var elapsed float64
	for _, r := range results {
		className, name := splitTestName(r.Name, testClass)
		c := testCase{ClassName: className, Name: name, Time: fmt.Sprintf("%.3f", r.Elapsed), SystemOut: strings.TrimSpace(r.Output)}
		suite.Tests++
		elapsed += r.Elapsed
		switch r.Status {
		case "fail":
			suite.Failures++
			c.Failure = &failure{Message: "Go test failed", Output: r.Output}
		case "skip":
			suite.Skipped++
			c.Skipped = &skipped{Message: skipMessage(r.Output)}
		}
		suite.Cases = append(suite.Cases, c)
	}
	suite.Time = fmt.Sprintf("%.3f", elapsed)
	writeXML(*output, testSuites{Suites: []testSuite{suite}})
}

func loadMigrationTests() (map[string]string, error) {
	out := map[string]string{}
	for file, class := range fileToClass {
		path := file
		if _, err := os.Stat(path); err != nil {
			// Allow running from repo root via go/ prefix.
			alt := filepath.Join("go", file)
			if _, err2 := os.Stat(alt); err2 != nil {
				continue
			}
			path = alt
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Test") || fn.Body == nil {
				continue
			}
			out[fn.Name.Name] = class
		}
	}
	return out, nil
}

func readResults() map[string]*result {
	items := map[string]*result{}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var e event
		if json.Unmarshal(scanner.Bytes(), &e) != nil || e.Test == "" {
			continue
		}
		r := items[e.Test]
		if r == nil {
			r = &result{Name: e.Test}
			items[e.Test] = r
		}
		r.Output += e.Output
		if e.Action == "pass" || e.Action == "fail" || e.Action == "skip" {
			r.Status, r.Elapsed = e.Action, e.Elapsed
		}
	}
	if err := scanner.Err(); err != nil {
		fatal(err)
	}
	return items
}

func leafResults(items map[string]*result, testClass map[string]string) []result {
	results := make([]result, 0, len(items))
	for name, r := range items {
		if r.Status == "" || strings.Contains(name, "/") {
			continue
		}
		if _, ok := testClass[name]; !ok {
			continue
		}
		results = append(results, *r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })
	return results
}

func splitTestName(name string, testClass map[string]string) (string, string) {
	if class, ok := testClass[name]; ok {
		return "s3tests." + class, name
	}
	return "s3tests", name
}

func skipMessage(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 || lines[0] == "" {
		return "skipped"
	}
	return strings.TrimSpace(lines[len(lines)-1])
}

func writeXML(path string, data testSuites) {
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	if _, err = f.WriteString(xml.Header); err != nil {
		fatal(err)
	}
	enc := xml.NewEncoder(f)
	enc.Indent("", "  ")
	if err = enc.Encode(data); err != nil {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
