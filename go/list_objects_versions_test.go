package s3tests

import (
	"context"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestBucketListVersionsMany(t *testing.T) {
	t.Parallel()

	testVersionsMany(t)
}
func TestBucketListVersionsDelimiterBasic(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_basic")
}
func TestBucketListVersionsEncodingBasic(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_encoding_basic")
}
func TestBucketListVersionsDelimiterPrefix(t *testing.T) {
	t.Parallel()

	testVersionsDelimiterPages(t, "test_bucket_list_versions_delimiter_prefix")
}
func TestBucketListVersionsDelimiterPrefixEndsWithDelimiter(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_prefix_ends_with_delimiter")
}
func TestBucketListVersionsDelimiterAlt(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_alt")
}
func TestBucketListVersionsDelimiterPrefixUnderscore(t *testing.T) {
	t.Parallel()

	testVersionsDelimiterPages(t, "test_bucket_list_versions_delimiter_prefix_underscore")
}
func TestBucketListVersionsDelimiterPercentage(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_percentage")
}
func TestBucketListVersionsDelimiterWhitespace(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_whitespace")
}
func TestBucketListVersionsDelimiterDot(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_dot")
}
func TestBucketListVersionsDelimiterUnreadable(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_unreadable")
}
func TestBucketListVersionsDelimiterEmpty(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_empty")
}
func TestBucketListVersionsDelimiterNone(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_none")
}
func TestBucketListVersionsDelimiterNotExist(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_delimiter_not_exist")
}
func TestBucketListVersionsDelimiterNotSkipSpecial(t *testing.T) {
	t.Parallel()

	testVersionsDelimiterBoundary(t)
}
func TestBucketListVersionsPrefixBasic(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_basic")
}
func TestBucketListVersionsPrefixAlt(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_alt")
}
func TestBucketListVersionsPrefixEmpty(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_empty")
}
func TestBucketListVersionsPrefixNone(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_none")
}
func TestBucketListVersionsPrefixNotExist(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_not_exist")
}
func TestBucketListVersionsPrefixUnreadable(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_unreadable")
}
func TestBucketListVersionsPrefixDelimiterBasic(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_basic")
}
func TestBucketListVersionsPrefixDelimiterAlt(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_alt")
}
func TestBucketListVersionsPrefixDelimiterPrefixNotExist(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_prefix_not_exist")
}
func TestBucketListVersionsPrefixDelimiterDelimiterNotExist(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_delimiter_not_exist")
}
func TestBucketListVersionsPrefixDelimiterPrefixDelimiterNotExist(t *testing.T) {
	t.Parallel()

	testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_prefix_delimiter_not_exist")
}
func TestBucketListVersionsMaxKeysOne(t *testing.T) {
	t.Parallel()

	testVersionsMax(t, "test_bucket_list_versions_max_keys_one")
}
func TestBucketListVersionsMaxKeysZero(t *testing.T) {
	t.Parallel()

	testVersionsMax(t, "test_bucket_list_versions_max_keys_zero")
}
func TestBucketListVersionsMaxKeysNone(t *testing.T) {
	t.Parallel()

	testVersionsMax(t, "test_bucket_list_versions_max_keys_none")
}
func TestBucketListVersionsMarkerNone(t *testing.T) {
	t.Parallel()

	testVersionsMarker(t, "test_bucket_list_versions_marker_none")
}
func TestBucketListVersionsMarkerEmpty(t *testing.T) {
	t.Parallel()

	testVersionsMarker(t, "test_bucket_list_versions_marker_empty")
}
func TestBucketListVersionsMarkerUnreadable(t *testing.T) {
	t.Parallel()

	testVersionsMarker(t, "test_bucket_list_versions_marker_unreadable")
}
func TestBucketListVersionsMarkerNotInList(t *testing.T) {
	t.Parallel()

	testVersionsMarker(t, "test_bucket_list_versions_marker_not_in_list")
}
func TestBucketListVersionsMarkerAfterList(t *testing.T) {
	t.Parallel()

	testVersionsMarker(t, "test_bucket_list_versions_marker_after_list")
}
func TestBucketListVersionsReturnData(t *testing.T) {
	t.Parallel()

	testVersionsReturnData(t)
}
func TestBucketListVersionsObjectsAnonymous(t *testing.T) {
	t.Parallel()

	testVersionsAnonymous(t, "test_bucket_list_versions_objects_anonymous")
}
func TestBucketListVersionsObjectsAnonymousFail(t *testing.T) {
	t.Parallel()

	testVersionsAnonymous(t, "test_bucket_list_versions_objects_anonymous_fail")
}
func TestBucketListVersionsNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}
func TestVersioningBucketListFilteringAll(t *testing.T) {
	t.Parallel()

	testVersionsFiltering(t)
}
func TestVersioningObjListMarker(t *testing.T) {
	t.Parallel()

	testVersionsOrder(t)
}

func listVersions(t *testing.T, client *s3.Client, input *s3.ListObjectVersionsInput) *s3.ListObjectVersionsOutput {
	t.Helper()
	out, err := client.ListObjectVersions(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func versionKeys(out *s3.ListObjectVersionsOutput, decode bool) []string {
	result := make([]string, 0, len(out.Versions))
	for _, item := range out.Versions {
		key := aws.ToString(item.Key)
		if decode {
			key, _ = url.QueryUnescape(key)
		}
		result = append(result, key)
	}
	return result
}
func versionPrefixes(out *s3.ListObjectVersionsOutput, decode bool) []string {
	result := make([]string, 0, len(out.CommonPrefixes))
	for _, item := range out.CommonPrefixes {
		key := aws.ToString(item.Prefix)
		if decode {
			key, _ = url.QueryUnescape(key)
		}
		result = append(result, key)
	}
	return result
}

func testVersionsMany(t *testing.T) {
	s, b := listFixture(t, []string{"foo", "bar", "baz"})
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), MaxKeys: aws.Int32(2)})
	assertStringList(t, versionKeys(out, false), []string{"bar", "baz"})
	if !aws.ToBool(out.IsTruncated) {
		t.Fatal("not truncated")
	}
	out = listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String("baz"), MaxKeys: aws.Int32(2)})
	assertStringList(t, versionKeys(out, false), []string{"foo"})
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("last page truncated")
	}
}

type versionListCase struct {
	keys, wantKeys, wantPrefixes      []string
	delimiter, prefix                 string
	setDelimiter, setPrefix, encoding bool
}

func testVersionsSimple(t *testing.T, name string) {
	t.Helper()
	c := versionListCase{keys: []string{"bar", "baz", "cab", "foo"}, wantKeys: []string{"bar", "baz", "cab", "foo"}}
	switch name {
	case "test_bucket_list_versions_delimiter_basic":
		c = versionListCase{[]string{"foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"}, []string{"asdf"}, []string{"foo/", "quux/"}, "/", "", true, false, false}
	case "test_bucket_list_versions_encoding_basic":
		c = versionListCase{[]string{"foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"}, []string{"asdf+b"}, []string{"foo+1/", "foo/", "quux ab/"}, "/", "", true, false, true}
	case "test_bucket_list_versions_delimiter_prefix_ends_with_delimiter":
		c = versionListCase{[]string{"asdf/"}, []string{"asdf/"}, nil, "/", "asdf/", true, true, false}
	case "test_bucket_list_versions_delimiter_alt":
		c.delimiter, c.setDelimiter, c.wantKeys, c.wantPrefixes = "a", true, []string{"foo"}, []string{"ba", "ca"}
	case "test_bucket_list_versions_delimiter_percentage":
		c.keys = []string{"b%ar", "b%az", "c%ab", "foo"}
		c.delimiter, c.setDelimiter, c.wantKeys, c.wantPrefixes = "%", true, []string{"foo"}, []string{"b%", "c%"}
	case "test_bucket_list_versions_delimiter_whitespace":
		c.keys = []string{"b ar", "b az", "c ab", "foo"}
		c.delimiter, c.setDelimiter, c.wantKeys, c.wantPrefixes = " ", true, []string{"foo"}, []string{"b ", "c "}
	case "test_bucket_list_versions_delimiter_dot":
		c.keys = []string{"b.ar", "b.az", "c.ab", "foo"}
		c.delimiter, c.setDelimiter, c.wantKeys, c.wantPrefixes = ".", true, []string{"foo"}, []string{"b.", "c."}
	case "test_bucket_list_versions_delimiter_unreadable":
		c.delimiter, c.setDelimiter = "\n", true
	case "test_bucket_list_versions_delimiter_empty":
		c.setDelimiter = true
	case "test_bucket_list_versions_delimiter_none":
	case "test_bucket_list_versions_delimiter_not_exist":
		c.delimiter, c.setDelimiter = "/", true
	case "test_bucket_list_versions_prefix_basic":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.prefix, c.setPrefix, c.wantKeys = "foo/", true, []string{"foo/bar", "foo/baz"}
	case "test_bucket_list_versions_prefix_alt":
		c.keys = []string{"bar", "baz", "foo"}
		c.prefix, c.setPrefix, c.wantKeys = "ba", true, []string{"bar", "baz"}
	case "test_bucket_list_versions_prefix_empty":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.wantKeys = c.keys
		c.setPrefix = true
	case "test_bucket_list_versions_prefix_none":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.wantKeys = c.keys
	case "test_bucket_list_versions_prefix_not_exist":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.prefix, c.setPrefix, c.wantKeys = "d", true, []string{}
	case "test_bucket_list_versions_prefix_unreadable":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.prefix, c.setPrefix, c.wantKeys = "\n", true, []string{}
	case "test_bucket_list_versions_prefix_delimiter_basic":
		c = versionListCase{[]string{"foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"}, []string{"foo/bar"}, []string{"foo/baz/"}, "/", "foo/", true, true, false}
	case "test_bucket_list_versions_prefix_delimiter_alt":
		c = versionListCase{[]string{"bar", "bazar", "cab", "foo"}, []string{"bar"}, []string{"baza"}, "a", "ba", true, true, false}
	case "test_bucket_list_versions_prefix_delimiter_prefix_not_exist":
		c = versionListCase{[]string{"b/a/r", "b/a/c", "b/a/g", "g"}, []string{}, nil, "d", "/", true, true, false}
	case "test_bucket_list_versions_prefix_delimiter_delimiter_not_exist":
		c = versionListCase{[]string{"b/a/c", "b/a/g", "b/a/r", "g"}, []string{"b/a/c", "b/a/g", "b/a/r"}, nil, "z", "b", true, true, false}
	case "test_bucket_list_versions_prefix_delimiter_prefix_delimiter_not_exist":
		c = versionListCase{[]string{"b/a/r", "b/a/c", "b/a/g", "g"}, []string{}, nil, "z", "y", true, true, false}
	default:
		t.Fatalf("unimplemented version list case %q", name)
	}
	s, b := listFixture(t, c.keys)
	input := &s3.ListObjectVersionsInput{Bucket: aws.String(b)}
	if c.setDelimiter {
		input.Delimiter = aws.String(c.delimiter)
	}
	if c.setPrefix {
		input.Prefix = aws.String(c.prefix)
	}
	if c.encoding {
		input.EncodingType = types.EncodingTypeUrl
	}
	out := listVersions(t, s.client, input)
	gotK, gotP := versionKeys(out, c.encoding), versionPrefixes(out, c.encoding)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(c.wantKeys)
	sort.Strings(c.wantPrefixes)
	assertStringList(t, gotK, c.wantKeys)
	assertStringList(t, gotP, c.wantPrefixes)
	if c.setDelimiter && aws.ToString(out.Delimiter) != c.delimiter {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if c.setPrefix && aws.ToString(out.Prefix) != c.prefix {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

func testVersionsDelimiterPages(t *testing.T, name string) {
	keys := []string{"asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"}
	object, p1, p2, nested, nestedP := "asdf", "boo/", "cquux/", "boo/bar", "boo/baz/"
	if strings.Contains(name, "underscore") {
		keys = []string{"Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"}
		object, p1, p2, nested, nestedP = "Obj1_", "Under1/", "Under2/", "Under1/bar", "Under1/baz/"
	}
	s, b := listFixture(t, keys)
	check := func(prefix, marker string, max int32, wk, wp []string, truncated bool, wantNext string) string {
		out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(prefix), Delimiter: aws.String("/"), KeyMarker: aws.String(marker), MaxKeys: aws.Int32(max)})
		assertStringList(t, versionKeys(out, false), wk)
		assertStringList(t, versionPrefixes(out, false), wp)
		if aws.ToBool(out.IsTruncated) != truncated || aws.ToString(out.NextKeyMarker) != wantNext {
			t.Fatalf("truncated=%v next=%q", out.IsTruncated, aws.ToString(out.NextKeyMarker))
		}
		return aws.ToString(out.NextKeyMarker)
	}
	m := check("", "", 1, []string{object}, nil, true, object)
	m = check("", m, 1, nil, []string{p1}, true, p1)
	check("", m, 1, nil, []string{p2}, false, "")
	m = check("", "", 2, []string{object}, []string{p1}, true, p1)
	check("", m, 2, nil, []string{p2}, false, "")
	m = check(p1, "", 1, []string{nested}, nil, true, nested)
	check(p1, m, 1, nil, []string{nestedP}, false, "")
	check(p1, "", 2, []string{nested}, []string{nestedP}, false, "")
}

func testVersionsDelimiterBoundary(t *testing.T) {
	keys := make([]string, 0, 1003)
	for i := 1000; i < 1999; i++ {
		keys = append(keys, "0/"+strconv.Itoa(i))
	}
	tail := []string{"1999", "1999#", "1999+", "2000"}
	keys = append(keys, tail...)
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("/")})
	assertStringList(t, versionKeys(out, false), tail)
	assertStringList(t, versionPrefixes(out, false), []string{"0/"})
}

func testVersionsMax(t *testing.T, name string) {
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	input := &s3.ListObjectVersionsInput{Bucket: aws.String(b)}
	if strings.HasSuffix(name, "one") {
		input.MaxKeys = aws.Int32(1)
	} else if strings.HasSuffix(name, "zero") {
		input.MaxKeys = aws.Int32(0)
	}
	out := listVersions(t, s.client, input)
	if strings.HasSuffix(name, "one") {
		assertStringList(t, versionKeys(out, false), keys[:1])
		if !aws.ToBool(out.IsTruncated) {
			t.Fatal("not truncated")
		}
		out = listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String(keys[0])})
		assertStringList(t, versionKeys(out, false), keys[1:])
	} else if strings.HasSuffix(name, "zero") {
		if len(out.Versions) != 0 || aws.ToBool(out.IsTruncated) {
			t.Fatalf("versions=%v truncated=%v", out.Versions, out.IsTruncated)
		}
	} else {
		assertStringList(t, versionKeys(out, false), keys)
		if aws.ToInt32(out.MaxKeys) != 1000 {
			t.Fatalf("MaxKeys=%v", out.MaxKeys)
		}
	}
}

func testVersionsMarker(t *testing.T, name string) {
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	marker := ""
	want := keys
	if strings.HasSuffix(name, "unreadable") {
		marker = "\n"
	} else if strings.HasSuffix(name, "not_in_list") {
		marker = "blah"
		want = []string{"foo", "quxx"}
	} else if strings.HasSuffix(name, "after_list") {
		marker = "zzz"
		want = []string{}
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String(marker)})
	assertStringList(t, versionKeys(out, false), want)
	if aws.ToString(out.NextKeyMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextKeyMarker), out.IsTruncated)
	}
	if marker != "" && aws.ToString(out.KeyMarker) != marker {
		t.Fatalf("marker=%q", aws.ToString(out.KeyMarker))
	}
}

func testVersionsReturnData(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	for _, key := range []string{"bar", "baz", "foo"} {
		put(t, s, b, key, key, nil)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	for _, version := range out.Versions {
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: version.Key, VersionId: version.VersionId})
		if err != nil || aws.ToString(head.ETag) != aws.ToString(version.ETag) || aws.ToInt64(head.ContentLength) != aws.ToInt64(version.Size) || !aws.ToBool(version.IsLatest) {
			t.Fatalf("version=%#v head=%#v err=%v", version, head, err)
		}
	}
}

func testVersionsAnonymous(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	if name == "test_bucket_list_versions_objects_anonymous" {
		b = ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
		_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(b), ACL: types.BucketCannedACLPublicRead})
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := anonymousClient(s).ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if strings.HasSuffix(name, "fail") {
		assertS3Error(t, err, 403, "AccessDenied")
	} else if err != nil {
		t.Fatal(err)
	}
}

func testVersionsFiltering(t *testing.T) {
	s, b := listFixture(t, []string{"test1/f1", "test2/f2", "test3", "test4/f3", "testF4"})
	input := &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("/"), MaxKeys: aws.Int32(3)}
	one := listVersions(t, s.client, input)
	assertStringList(t, versionKeys(one, false), []string{"test3"})
	assertStringList(t, versionPrefixes(one, false), []string{"test1/", "test2/"})
	if !aws.ToBool(one.IsTruncated) || aws.ToString(one.NextKeyMarker) != "test3" {
		t.Fatalf("first=%#v", one)
	}
	input.KeyMarker = aws.String("test3")
	two := listVersions(t, s.client, input)
	if aws.ToBool(two.IsTruncated) {
		t.Fatal("second page truncated")
	}
}

func testVersionsOrder(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	key := "testVersioningObjListMarker"
	want := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		out := put(t, s, b, key, key+strconv.Itoa(i), nil)
		want = append([]string{aws.ToString(out.VersionId)}, want...)
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(listed.Versions) != len(want) {
		t.Fatalf("versions=%d want=%d", len(listed.Versions), len(want))
	}
	for i, id := range want {
		if aws.ToString(listed.Versions[i].VersionId) != id || aws.ToBool(listed.Versions[i].IsLatest) != (i == 0) {
			t.Fatalf("version[%d]=%q latest=%v want=%q", i, aws.ToString(listed.Versions[i].VersionId), listed.Versions[i].IsLatest, id)
		}
	}
}
