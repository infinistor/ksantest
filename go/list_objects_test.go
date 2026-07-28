package s3tests

import (
	"context"
	"net/url"
	"sort"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestBucketListMany(t *testing.T) {
	t.Parallel()

	testListObjectsMany(t)
}
func TestBucketListDelimiterBasic(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_basic")
}
func TestBucketListEncodingBasic(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_encoding_basic")
}
func TestBucketListDelimiterPrefix(t *testing.T) {
	t.Parallel()

	testListDelimiterPagination(t, "test_bucket_list_delimiter_prefix")
}
func TestBucketListDelimiterPrefixEndsWithDelimiter(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_prefix_ends_with_delimiter")
}
func TestBucketListDelimiterAlt(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_alt")
}
func TestBucketListDelimiterPrefixUnderscore(t *testing.T) {
	t.Parallel()

	testListDelimiterPagination(t, "test_bucket_list_delimiter_prefix_underscore")
}
func TestBucketListDelimiterPercentage(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_percentage")
}
func TestBucketListDelimiterWhitespace(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_whitespace")
}
func TestBucketListDelimiterDot(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_dot")
}
func TestBucketListDelimiterUnreadable(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_unreadable")
}
func TestBucketListDelimiterEmpty(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_empty")
}
func TestBucketListDelimiterNone(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_none")
}
func TestBucketListDelimiterNotExist(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_delimiter_not_exist")
}
func TestBucketListDelimiterNotSkipSpecial(t *testing.T) {
	t.Parallel()

	testListDelimiterBoundary(t)
}
func TestBucketListPrefixBasic(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_basic")
}
func TestBucketListPrefixAlt(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_alt")
}
func TestBucketListPrefixEmpty(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_empty")
}
func TestBucketListPrefixNone(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_none")
}
func TestBucketListPrefixNotExist(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_not_exist")
}
func TestBucketListPrefixUnreadable(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_unreadable")
}
func TestBucketListPrefixDelimiterBasic(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_delimiter_basic")
}
func TestBucketListPrefixDelimiterAlt(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_delimiter_alt")
}
func TestBucketListPrefixDelimiterPrefixNotExist(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_delimiter_prefix_not_exist")
}
func TestBucketListPrefixDelimiterDelimiterNotExist(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_delimiter_delimiter_not_exist")
}
func TestBucketListPrefixDelimiterPrefixDelimiterNotExist(t *testing.T) {
	t.Parallel()

	testListSimple(t, "test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist")
}
func TestBucketListMaxKeysOne(t *testing.T) {
	t.Parallel()

	testListMaxKeys(t, "test_bucket_list_max_keys_one")
}
func TestBucketListMaxKeysZero(t *testing.T) {
	t.Parallel()

	testListMaxKeys(t, "test_bucket_list_max_keys_zero")
}
func TestBucketListMaxKeysNone(t *testing.T) {
	t.Parallel()

	testListMaxKeys(t, "test_bucket_list_max_keys_none")
}
func TestBucketListMarkerNone(t *testing.T) {
	t.Parallel()

	testListMarker(t, "test_bucket_list_marker_none")
}
func TestBucketListMarkerEmpty(t *testing.T) {
	t.Parallel()

	testListMarker(t, "test_bucket_list_marker_empty")
}
func TestBucketListMarkerUnreadable(t *testing.T) {
	t.Parallel()

	testListMarker(t, "test_bucket_list_marker_unreadable")
}
func TestBucketListMarkerNotInList(t *testing.T) {
	t.Parallel()

	testListMarker(t, "test_bucket_list_marker_not_in_list")
}
func TestBucketListMarkerAfterList(t *testing.T) {
	t.Parallel()

	testListMarker(t, "test_bucket_list_marker_after_list")
}
func TestBucketListReturnData(t *testing.T) {
	t.Parallel()

	testListReturnData(t)
}
func TestBucketListObjectsAnonymous(t *testing.T) {
	t.Parallel()

	testListAnonymous(t, "test_bucket_list_objects_anonymous")
}
func TestBucketListObjectsAnonymousFail(t *testing.T) {
	t.Parallel()

	testListAnonymous(t, "test_bucket_list_objects_anonymous_fail")
}
func TestBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}
func TestBucketListFilteringAll(t *testing.T) {
	t.Parallel()

	testListFilteringAll(t)
}
func TestBucketListVersioning(t *testing.T) {
	t.Parallel()

	testListVersioning(t)
}

func listFixture(t *testing.T, keys []string) (*suite, string) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	for _, key := range keys {
		put(t, s, bucket, key, key, nil)
	}
	return s, bucket
}

func listV1(t *testing.T, client *s3.Client, input *s3.ListObjectsInput) *s3.ListObjectsOutput {
	t.Helper()
	out, err := client.ListObjects(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func listKeys(out *s3.ListObjectsOutput, decode bool) []string {
	keys := make([]string, 0, len(out.Contents))
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if decode {
			if value, err := url.QueryUnescape(key); err == nil {
				key = value
			}
		}
		keys = append(keys, key)
	}
	return keys
}

func listPrefixes(out *s3.ListObjectsOutput, decode bool) []string {
	prefixes := make([]string, 0, len(out.CommonPrefixes))
	for _, item := range out.CommonPrefixes {
		prefix := aws.ToString(item.Prefix)
		if decode {
			if value, err := url.QueryUnescape(prefix); err == nil {
				prefix = value
			}
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes
}

func assertStringList(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got=%q want=%q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got=%q want=%q", got, want)
		}
	}
}

func testListObjectsMany(t *testing.T) {
	s, bucket := listFixture(t, []string{"foo", "bar", "baz"})
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), MaxKeys: aws.Int32(2)})
	assertStringList(t, listKeys(out, false), []string{"bar", "baz"})
	if !aws.ToBool(out.IsTruncated) {
		t.Fatal("first page is not truncated")
	}
	out = listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String("baz"), MaxKeys: aws.Int32(2)})
	assertStringList(t, listKeys(out, false), []string{"foo"})
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("last page is truncated")
	}
}

func testListSimple(t *testing.T, name string) {
	t.Helper()
	keys := []string{"bar", "baz", "cab", "foo"}
	delimiter, prefix := "", ""
	setDelimiter, setPrefix, encoding := false, false, false
	wantKeys, wantPrefixes := append([]string(nil), keys...), []string{}
	switch name {
	case "test_bucket_list_delimiter_basic":
		keys = []string{"foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"}
		delimiter = "/"
		setDelimiter = true
		wantKeys = []string{"asdf"}
		wantPrefixes = []string{"foo/", "quux/"}
	case "test_bucket_list_encoding_basic":
		keys = []string{"foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"}
		delimiter = "/"
		setDelimiter, encoding = true, true
		wantKeys = []string{"asdf+b"}
		wantPrefixes = []string{"foo+1/", "foo/", "quux ab/"}
	case "test_bucket_list_delimiter_prefix_ends_with_delimiter":
		keys = []string{"asdf/"}
		delimiter, prefix = "/", "asdf/"
		setDelimiter, setPrefix = true, true
		wantKeys = []string{"asdf/"}
	case "test_bucket_list_delimiter_alt":
		delimiter = "a"
		setDelimiter = true
		wantKeys = []string{"foo"}
		wantPrefixes = []string{"ba", "ca"}
	case "test_bucket_list_delimiter_percentage":
		keys = []string{"b%ar", "b%az", "c%ab", "foo"}
		delimiter = "%"
		setDelimiter = true
		wantKeys = []string{"foo"}
		wantPrefixes = []string{"b%", "c%"}
	case "test_bucket_list_delimiter_whitespace":
		keys = []string{"b ar", "b az", "c ab", "foo"}
		delimiter = " "
		setDelimiter = true
		wantKeys = []string{"foo"}
		wantPrefixes = []string{"b ", "c "}
	case "test_bucket_list_delimiter_dot":
		keys = []string{"b.ar", "b.az", "c.ab", "foo"}
		delimiter = "."
		setDelimiter = true
		wantKeys = []string{"foo"}
		wantPrefixes = []string{"b.", "c."}
	case "test_bucket_list_delimiter_unreadable":
		delimiter = "\n"
		setDelimiter = true
	case "test_bucket_list_delimiter_empty":
		setDelimiter = true
	case "test_bucket_list_delimiter_none":
	case "test_bucket_list_delimiter_not_exist":
		delimiter = "/"
		setDelimiter = true
	case "test_bucket_list_prefix_basic":
		keys = []string{"foo/bar", "foo/baz", "quux"}
		prefix = "foo/"
		setPrefix = true
		wantKeys = []string{"foo/bar", "foo/baz"}
	case "test_bucket_list_prefix_alt":
		keys = []string{"bar", "baz", "foo"}
		prefix = "ba"
		setPrefix = true
		wantKeys = []string{"bar", "baz"}
	case "test_bucket_list_prefix_empty":
		keys = []string{"foo/bar", "foo/baz", "quux"}
		setPrefix = true
		wantKeys = keys
	case "test_bucket_list_prefix_none":
		keys = []string{"foo/bar", "foo/baz", "quux"}
		wantKeys = keys
	case "test_bucket_list_prefix_not_exist":
		keys = []string{"foo/bar", "foo/baz", "quux"}
		prefix = "d"
		setPrefix = true
		wantKeys = []string{}
	case "test_bucket_list_prefix_unreadable":
		keys = []string{"foo/bar", "foo/baz", "quux"}
		prefix = "\n"
		setPrefix = true
		wantKeys = []string{}
	case "test_bucket_list_prefix_delimiter_basic":
		keys = []string{"foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"}
		prefix, delimiter = "foo/", "/"
		setPrefix, setDelimiter = true, true
		wantKeys = []string{"foo/bar"}
		wantPrefixes = []string{"foo/baz/"}
	case "test_bucket_list_prefix_delimiter_alt":
		keys = []string{"bar", "bazar", "cab", "foo"}
		prefix, delimiter = "ba", "a"
		setPrefix, setDelimiter = true, true
		wantKeys = []string{"bar"}
		wantPrefixes = []string{"baza"}
	case "test_bucket_list_prefix_delimiter_prefix_not_exist":
		keys = []string{"b/a/r", "b/a/c", "b/a/g", "g"}
		prefix, delimiter = "/", "d"
		setPrefix, setDelimiter = true, true
		wantKeys = []string{}
	case "test_bucket_list_prefix_delimiter_delimiter_not_exist":
		keys = []string{"b/a/c", "b/a/g", "b/a/r", "g"}
		prefix, delimiter = "b", "z"
		setPrefix, setDelimiter = true, true
		wantKeys = []string{"b/a/c", "b/a/g", "b/a/r"}
	case "test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist":
		keys = []string{"b/a/r", "b/a/c", "b/a/g", "g"}
		prefix, delimiter = "y", "z"
		setPrefix, setDelimiter = true, true
		wantKeys = []string{}
	default:
		t.Fatalf("unimplemented simple list case %q", name)
	}
	s, bucket := listFixture(t, keys)
	input := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	if setDelimiter {
		input.Delimiter = aws.String(delimiter)
	}
	if setPrefix {
		input.Prefix = aws.String(prefix)
	}
	if encoding {
		input.EncodingType = types.EncodingTypeUrl
	}
	out := listV1(t, s.client, input)
	gotKeys, gotPrefixes := listKeys(out, encoding), listPrefixes(out, encoding)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if setDelimiter && aws.ToString(out.Delimiter) != delimiter {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), delimiter)
	}
	if setPrefix && aws.ToString(out.Prefix) != prefix {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), prefix)
	}
}

func testListDelimiterPagination(t *testing.T, name string) {
	t.Helper()
	keys := []string{"asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"}
	object, prefixOne, prefixTwo := "asdf", "boo/", "cquux/"
	nestedObject, nestedPrefix := "boo/bar", "boo/baz/"
	if name == "test_bucket_list_delimiter_prefix_underscore" {
		keys = []string{"Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"}
		object, prefixOne, prefixTwo = "Obj1_", "Under1/", "Under2/"
		nestedObject, nestedPrefix = "Under1/bar", "Under1/baz/"
	}
	s, bucket := listFixture(t, keys)
	check := func(prefix, marker string, max int32, wantObjects, wantPrefixes []string, truncated bool, wantMarker string) {
		input := &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix), Delimiter: aws.String("/"), MaxKeys: aws.Int32(max)}
		if marker != "" {
			input.Marker = aws.String(marker)
		}
		out := listV1(t, s.client, input)
		assertStringList(t, listKeys(out, false), wantObjects)
		assertStringList(t, listPrefixes(out, false), wantPrefixes)
		if aws.ToBool(out.IsTruncated) != truncated || aws.ToString(out.NextMarker) != wantMarker {
			t.Fatalf("truncated=%v marker=%q want=%v/%q", aws.ToBool(out.IsTruncated), aws.ToString(out.NextMarker), truncated, wantMarker)
		}
	}
	check("", "", 1, []string{object}, nil, true, object)
	check("", object, 1, nil, []string{prefixOne}, true, prefixOne)
	check("", prefixOne, 1, nil, []string{prefixTwo}, false, "")
	check("", "", 2, []string{object}, []string{prefixOne}, true, prefixOne)
	check("", prefixOne, 2, nil, []string{prefixTwo}, false, "")
	check(prefixOne, "", 1, []string{nestedObject}, nil, true, nestedObject)
	check(prefixOne, nestedObject, 1, nil, []string{nestedPrefix}, false, "")
	check(prefixOne, "", 2, []string{nestedObject}, []string{nestedPrefix}, false, "")
}

func testListDelimiterBoundary(t *testing.T) {
	keys := make([]string, 0, 1003)
	for i := 1000; i < 1999; i++ {
		keys = append(keys, "0/"+strconv.Itoa(i))
	}
	tail := []string{"1999", "1999#", "1999+", "2000"}
	keys = append(keys, tail...)
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("/")})
	assertStringList(t, listKeys(out, false), tail)
	assertStringList(t, listPrefixes(out, false), []string{"0/"})
}

func testListMaxKeys(t *testing.T, name string) {
	t.Helper()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	input := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	if name == "test_bucket_list_max_keys_one" {
		input.MaxKeys = aws.Int32(1)
	} else if name == "test_bucket_list_max_keys_zero" {
		input.MaxKeys = aws.Int32(0)
	}
	out := listV1(t, s.client, input)
	switch name {
	case "test_bucket_list_max_keys_one":
		assertStringList(t, listKeys(out, false), keys[:1])
		if !aws.ToBool(out.IsTruncated) {
			t.Fatal("not truncated")
		}
		next := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String(keys[0])})
		assertStringList(t, listKeys(next, false), keys[1:])
	case "test_bucket_list_max_keys_zero":
		if len(out.Contents) != 0 || aws.ToBool(out.IsTruncated) {
			t.Fatalf("contents=%v truncated=%v", out.Contents, aws.ToBool(out.IsTruncated))
		}
	default:
		assertStringList(t, listKeys(out, false), keys)
		if aws.ToInt32(out.MaxKeys) != 1000 || aws.ToBool(out.IsTruncated) {
			t.Fatalf("max=%v truncated=%v", out.MaxKeys, aws.ToBool(out.IsTruncated))
		}
	}
}

func testListMarker(t *testing.T, name string) {
	t.Helper()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	marker := ""
	want := keys
	switch name {
	case "test_bucket_list_marker_unreadable":
		marker = "\n"
	case "test_bucket_list_marker_not_in_list":
		marker = "blah"
		want = []string{"foo", "quxx"}
	case "test_bucket_list_marker_after_list":
		marker = "zzz"
		want = []string{}
	}
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String(marker)})
	assertStringList(t, listKeys(out, false), want)
	if aws.ToString(out.NextMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextMarker), aws.ToBool(out.IsTruncated))
	}
	if marker != "" && aws.ToString(out.Marker) != marker {
		t.Fatalf("marker=%q want=%q", aws.ToString(out.Marker), marker)
	}
}

func testListReturnData(t *testing.T) {
	s, bucket := listFixture(t, []string{"bar", "baz", "foo"})
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToString(object.ETag) != aws.ToString(head.ETag) || object.LastModified == nil || head.LastModified == nil || !object.LastModified.Equal(*head.LastModified) || aws.ToInt64(object.Size) != aws.ToInt64(head.ContentLength) {
			t.Fatalf("object=%#v head=%#v", object, head)
		}
	}
}

func testListAnonymous(t *testing.T, name string) {
	s := newSuite(t)
	bucket := s.bucket(t)
	if name == "test_bucket_list_objects_anonymous" {
		bucket = ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
		_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicRead})
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := anonymousClient(s).ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if name == "test_bucket_list_objects_anonymous_fail" {
		assertS3Error(t, err, 403, "AccessDenied")
	} else if err != nil {
		t.Fatal(err)
	}
}

func testListFilteringAll(t *testing.T) {
	keys := []string{"test1/f1", "test2/f2", "test3", "test4/f3", "testF4"}
	s, bucket := listFixture(t, keys)
	input := &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("/"), MaxKeys: aws.Int32(3)}
	out := listV1(t, s.client, input)
	assertStringList(t, listKeys(out, false), []string{"test3"})
	assertStringList(t, listPrefixes(out, false), []string{"test1/", "test2/"})
	if !aws.ToBool(out.IsTruncated) || aws.ToString(out.NextMarker) != "test3" {
		t.Fatalf("truncated=%v marker=%q", aws.ToBool(out.IsTruncated), aws.ToString(out.NextMarker))
	}
	input.Marker = aws.String("test3")
	out = listV1(t, s.client, input)
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("second page is truncated")
	}
}

func testListVersioning(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	enableVersioning(t, s, bucket)
	keys := []string{"aaa", "bbb", "ccc"}
	for _, key := range keys {
		for i := 0; i < 3; i++ {
			put(t, s, bucket, key, key+strconv.Itoa(i), nil)
		}
	}
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	assertStringList(t, listKeys(out, false), keys)
}
