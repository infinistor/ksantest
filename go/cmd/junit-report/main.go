package main

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"os"
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

func main() {
	output := flag.String("output", "Result_go.xml", "JUnit XML output path")
	flag.Parse()
	results := leafResults(readResults())
	suite := testSuite{Name: "go"}
	var elapsed float64
	for _, r := range results {
		className, name := splitTestName(r.Name)
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

func leafResults(items map[string]*result) []result {
	results := make([]result, 0, len(items))
	for name, r := range items {
		if r.Status == "" || !isMigrationScenario(name) {
			continue
		}
		results = append(results, *r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })
	return results
}

func isMigrationScenario(name string) bool {
	className, _, found := strings.Cut(name, "/")
	if !found || strings.Count(name, "/") != 1 {
		return false
	}
	_, ok := migrationClasses[className]
	return ok
}

var migrationClasses = map[string]struct{}{
	"TestAccelerate": {}, "TestAccess": {}, "TestACL": {}, "TestAnalytics": {}, "TestBackend": {},
	"TestCopyObject": {}, "TestCors": {}, "TestCSE": {}, "TestDeleteBucket": {}, "TestDeleteObjects": {},
	"TestGetObject": {}, "TestGetObjectAttributes": {}, "TestGrants": {}, "TestInventory": {}, "TestLifeCycle": {},
	"TestListBuckets": {}, "TestListObjects": {}, "TestListObjectsV2": {}, "TestListObjectsVersions": {}, "TestLock": {},
	"TestLogging": {}, "TestMetrics": {}, "TestMultipart": {}, "TestNotification": {}, "TestOwnership": {},
	"TestPayment": {}, "TestPolicy": {}, "TestPost": {}, "TestPutBucket": {}, "TestPutObject": {},
	"TestReplication": {}, "TestSelectObjectContent": {}, "TestSSEC": {}, "TestSSES3": {}, "TestTaggings": {},
	"TestVersioning": {}, "TestWebsite": {},
}

func splitTestName(name string) (string, string) {
	className, method, found := strings.Cut(name, "/")
	if !found {
		return "s3tests", name
	}
	return "s3tests." + className, method
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
