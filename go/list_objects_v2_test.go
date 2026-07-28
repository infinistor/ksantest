package s3tests

import (
	"context"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestBucketListV2Many(t *testing.T) {
	t.Parallel()

	testListV2Many(t)
}
func TestBasicKeyCount(t *testing.T) {
	t.Parallel()

	s, b := listFixture(t, []string{"0", "1", "2", "3", "4"})
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if aws.ToInt32(out.KeyCount) != 5 {
		t.Fatalf("KeyCount=%v", out.KeyCount)
	}
}
func TestBucketListV2DelimiterBasic(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_basic")
}
func TestBucketListV2EncodingBasic(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_encoding_basic")
}
func TestBucketListV2DelimiterPrefix(t *testing.T) {
	t.Parallel()

	testListV2DelimiterPages(t, "test_bucket_list_v2_delimiter_prefix")
}
func TestBucketListV2DelimiterPrefixEndsWithDelimiter(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_prefix_ends_with_delimiter")
}
func TestBucketListV2DelimiterAlt(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_alt")
}
func TestBucketListV2DelimiterPrefixUnderscore(t *testing.T) {
	t.Parallel()

	testListV2DelimiterPages(t, "test_bucket_list_v2_delimiter_prefix_underscore")
}
func TestBucketListV2DelimiterPercentage(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_percentage")
}
func TestBucketListV2DelimiterWhitespace(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_whitespace")
}
func TestBucketListV2DelimiterDot(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_dot")
}
func TestBucketListV2DelimiterUnreadable(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_unreadable")
}
func TestBucketListV2DelimiterEmpty(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_empty")
}
func TestBucketListV2DelimiterNone(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_none")
}
func TestBucketListV2FetchOwnerNotEmpty(t *testing.T) {
	t.Parallel()

	testListV2Owner(t, "test_bucket_list_v2_fetch_owner_not_empty")
}
func TestBucketListV2FetchOwnerDefaultEmpty(t *testing.T) {
	t.Parallel()

	testListV2Owner(t, "test_bucket_list_v2_fetch_owner_default_empty")
}
func TestBucketListV2FetchOwnerEmpty(t *testing.T) {
	t.Parallel()

	testListV2Owner(t, "test_bucket_list_v2_fetch_owner_empty")
}
func TestBucketListV2DelimiterNotExist(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_delimiter_not_exist")
}
func TestBucketListV2PrefixBasic(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_basic")
}
func TestBucketListV2PrefixAlt(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_alt")
}
func TestBucketListV2PrefixEmpty(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_empty")
}
func TestBucketListV2PrefixNone(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_none")
}
func TestBucketListV2PrefixNotExist(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_not_exist")
}
func TestBucketListV2PrefixUnreadable(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_unreadable")
}
func TestBucketListV2PrefixDelimiterBasic(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_basic")
}
func TestBucketListV2PrefixDelimiterAlt(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_alt")
}
func TestBucketListV2PrefixDelimiterPrefixNotExist(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_prefix_not_exist")
}
func TestBucketListV2PrefixDelimiterDelimiterNotExist(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_delimiter_not_exist")
}
func TestBucketListV2PrefixDelimiterPrefixDelimiterNotExist(t *testing.T) {
	t.Parallel()

	testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_prefix_delimiter_not_exist")
}
func TestBucketListV2MaxKeysOne(t *testing.T) {
	t.Parallel()

	testListV2Max(t, "test_bucket_list_v2_max_keys_one")
}
func TestBucketListV2MaxKeysZero(t *testing.T) {
	t.Parallel()

	testListV2Max(t, "test_bucket_list_v2_max_keys_zero")
}
func TestBucketListV2MaxKeysNone(t *testing.T) {
	t.Parallel()

	testListV2Max(t, "test_bucket_list_v2_max_keys_none")
}
func TestBucketListV2ContinuationToken(t *testing.T) {
	t.Parallel()

	testListV2Token(t, "test_bucket_list_v2_continuation_token")
}
func TestBucketListV2BothContinuationTokenStartAfter(t *testing.T) {
	t.Parallel()

	testListV2Token(t, "test_bucket_list_v2_both_continuation_token_start_after")
}
func TestBucketListV2StartAfterUnreadable(t *testing.T) {
	t.Parallel()

	testListV2StartAfter(t, "test_bucket_list_v2_start_after_unreadable")
}
func TestBucketListV2StartAfterNotInList(t *testing.T) {
	t.Parallel()

	testListV2StartAfter(t, "test_bucket_list_v2_start_after_not_in_list")
}
func TestBucketListV2StartAfterAfterList(t *testing.T) {
	t.Parallel()

	testListV2StartAfter(t, "test_bucket_list_v2_start_after_after_list")
}
func TestBucketListV2ObjectsAnonymous(t *testing.T) {
	t.Parallel()

	testListV2Anonymous(t, "test_bucket_list_v2_objects_anonymous")
}
func TestBucketListV2ObjectsAnonymousFail(t *testing.T) {
	t.Parallel()

	testListV2Anonymous(t, "test_bucket_list_v2_objects_anonymous_fail")
}
func TestBucketV2NotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}
func TestBucketListV2FilteringAll(t *testing.T) {
	t.Parallel()

	testListV2Filtering(t)
}
func TestBucketListV2Versioning(t *testing.T) {
	t.Parallel()

	testListV2Versioning(t)
}

func listV2(t *testing.T, client *s3.Client, input *s3.ListObjectsV2Input) *s3.ListObjectsV2Output {
	t.Helper()
	out, err := client.ListObjectsV2(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func listV2Keys(out *s3.ListObjectsV2Output) []string {
	keys := make([]string, 0, len(out.Contents))
	for _, object := range out.Contents {
		keys = append(keys, aws.ToString(object.Key))
	}
	return keys
}
func listV2Prefixes(out *s3.ListObjectsV2Output) []string {
	prefixes := make([]string, 0, len(out.CommonPrefixes))
	for _, item := range out.CommonPrefixes {
		prefixes = append(prefixes, aws.ToString(item.Prefix))
	}
	return prefixes
}

func testListV2Many(t *testing.T) {
	s, b := listFixture(t, []string{"foo", "bar", "baz"})
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), MaxKeys: aws.Int32(2)})
	assertStringList(t, listV2Keys(out), []string{"bar", "baz"})
	if !aws.ToBool(out.IsTruncated) || aws.ToInt32(out.KeyCount) != 2 {
		t.Fatalf("truncated=%v count=%v", out.IsTruncated, out.KeyCount)
	}
	out = listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), StartAfter: aws.String("baz"), MaxKeys: aws.Int32(2)})
	assertStringList(t, listV2Keys(out), []string{"foo"})
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("last page truncated")
	}
}

type listV2Case struct {
	keys, wantKeys, wantPrefixes []string
	delimiter, prefix            string
	setDelimiter, setPrefix      bool
	encoding                     types.EncodingType
}

func testListV2Simple(t *testing.T, name string) {
	t.Helper()
	c := listV2Case{keys: []string{"bar", "baz", "cab", "foo"}, wantKeys: []string{"bar", "baz", "cab", "foo"}}
	switch name {
	case "test_bucket_list_v2_delimiter_basic":
		c = listV2Case{[]string{"foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"}, []string{"asdf"}, []string{"foo/", "quux/"}, "/", "", true, false, ""}
	case "test_bucket_list_v2_encoding_basic":
		c = listV2Case{[]string{"foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"}, []string{"asdf%2Bb"}, []string{"foo%2B1/", "foo/", "quux+ab/"}, "/", "", true, false, types.EncodingTypeUrl}
	case "test_bucket_list_v2_delimiter_prefix_ends_with_delimiter":
		c = listV2Case{[]string{"asdf/"}, []string{"asdf/"}, nil, "/", "asdf/", true, true, ""}
	case "test_bucket_list_v2_delimiter_alt":
		c.delimiter, c.setDelimiter, c.wantKeys, c.wantPrefixes = "a", true, []string{"foo"}, []string{"ba", "ca"}
	case "test_bucket_list_v2_delimiter_percentage":
		c.keys = []string{"b%ar", "b%az", "c%ab", "foo"}
		c.delimiter, c.setDelimiter, c.wantKeys, c.wantPrefixes = "%", true, []string{"foo"}, []string{"b%", "c%"}
	case "test_bucket_list_v2_delimiter_whitespace":
		c.keys = []string{"b ar", "b az", "c ab", "foo"}
		c.delimiter, c.setDelimiter, c.wantKeys, c.wantPrefixes = " ", true, []string{"foo"}, []string{"b ", "c "}
	case "test_bucket_list_v2_delimiter_dot":
		c.keys = []string{"b.ar", "b.az", "c.ab", "foo"}
		c.delimiter, c.setDelimiter, c.wantKeys, c.wantPrefixes = ".", true, []string{"foo"}, []string{"b.", "c."}
	case "test_bucket_list_v2_delimiter_unreadable":
		c.delimiter, c.setDelimiter = "\n", true
	case "test_bucket_list_v2_delimiter_empty":
		c.setDelimiter = true
	case "test_bucket_list_v2_delimiter_none":
	case "test_bucket_list_v2_delimiter_not_exist":
		c.delimiter, c.setDelimiter = "/", true
	case "test_bucket_list_v2_prefix_basic":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.prefix, c.setPrefix, c.wantKeys = "foo/", true, []string{"foo/bar", "foo/baz"}
	case "test_bucket_list_v2_prefix_alt":
		c.keys = []string{"bar", "baz", "foo"}
		c.prefix, c.setPrefix, c.wantKeys = "ba", true, []string{"bar", "baz"}
	case "test_bucket_list_v2_prefix_empty":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.wantKeys = c.keys
		c.setPrefix = true
	case "test_bucket_list_v2_prefix_none":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.wantKeys = c.keys
	case "test_bucket_list_v2_prefix_not_exist":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.prefix, c.setPrefix, c.wantKeys = "d", true, []string{}
	case "test_bucket_list_v2_prefix_unreadable":
		c.keys = []string{"foo/bar", "foo/baz", "quux"}
		c.prefix, c.setPrefix, c.wantKeys = "\n", true, []string{}
	case "test_bucket_list_v2_prefix_delimiter_basic":
		c = listV2Case{[]string{"foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"}, []string{"foo/bar"}, []string{"foo/baz/"}, "/", "foo/", true, true, ""}
	case "test_bucket_list_v2_prefix_delimiter_alt":
		c = listV2Case{[]string{"bar", "bazar", "cab", "foo"}, []string{"bar"}, []string{"baza"}, "a", "ba", true, true, ""}
	case "test_bucket_list_v2_prefix_delimiter_prefix_not_exist":
		c = listV2Case{[]string{"b/a/r", "b/a/c", "b/a/g", "g"}, []string{}, nil, "d", "/", true, true, ""}
	case "test_bucket_list_v2_prefix_delimiter_delimiter_not_exist":
		c = listV2Case{[]string{"b/a/c", "b/a/g", "b/a/r", "g"}, []string{"b/a/c", "b/a/g", "b/a/r"}, nil, "z", "b", true, true, ""}
	case "test_bucket_list_v2_prefix_delimiter_prefix_delimiter_not_exist":
		c = listV2Case{[]string{"b/a/r", "b/a/c", "b/a/g", "g"}, []string{}, nil, "z", "y", true, true, ""}
	default:
		t.Fatalf("unimplemented V2 simple case %q", name)
	}
	s, b := listFixture(t, c.keys)
	input := &s3.ListObjectsV2Input{Bucket: aws.String(b), EncodingType: c.encoding}
	if c.setDelimiter {
		input.Delimiter = aws.String(c.delimiter)
	}
	if c.setPrefix {
		input.Prefix = aws.String(c.prefix)
	}
	out := listV2(t, s.client, input)
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(c.wantKeys)
	sort.Strings(c.wantPrefixes)
	assertStringList(t, gotKeys, c.wantKeys)
	assertStringList(t, gotPrefixes, c.wantPrefixes)
	if c.setDelimiter && aws.ToString(out.Delimiter) != c.delimiter {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if c.setPrefix && aws.ToString(out.Prefix) != c.prefix {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

func testListV2DelimiterPages(t *testing.T, name string) {
	keys := []string{"asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"}
	object, p1, p2, nested, nestedP := "asdf", "boo/", "cquux/", "boo/bar", "boo/baz/"
	if name == "test_bucket_list_v2_delimiter_prefix_underscore" {
		keys = []string{"Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"}
		object, p1, p2, nested, nestedP = "Obj1_", "Under1/", "Under2/", "Under1/bar", "Under1/baz/"
	}
	s, b := listFixture(t, keys)
	check := func(prefix string, token *string, max int32, wk, wp []string, truncated bool) *string {
		out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String(prefix), Delimiter: aws.String("/"), ContinuationToken: token, MaxKeys: aws.Int32(max)})
		assertStringList(t, listV2Keys(out), wk)
		assertStringList(t, listV2Prefixes(out), wp)
		if aws.ToBool(out.IsTruncated) != truncated || aws.ToInt32(out.KeyCount) != int32(len(wk)+len(wp)) {
			t.Fatalf("truncated=%v count=%v", out.IsTruncated, out.KeyCount)
		}
		return out.NextContinuationToken
	}
	token := check("", nil, 1, []string{object}, nil, true)
	token = check("", token, 1, nil, []string{p1}, true)
	check("", token, 1, nil, []string{p2}, false)
	token = check("", nil, 2, []string{object}, []string{p1}, true)
	check("", token, 2, nil, []string{p2}, false)
	token = check(p1, nil, 1, []string{nested}, nil, true)
	check(p1, token, 1, nil, []string{nestedP}, false)
	check(p1, nil, 2, []string{nested}, []string{nestedP}, false)
}

func testListV2Owner(t *testing.T, name string) {
	s, b := listFixture(t, []string{"foo/bar", "foo/baz", "quux"})
	input := &s3.ListObjectsV2Input{Bucket: aws.String(b)}
	if name == "test_bucket_list_v2_fetch_owner_not_empty" {
		input.FetchOwner = aws.Bool(true)
	} else if name == "test_bucket_list_v2_fetch_owner_empty" {
		input.FetchOwner = aws.Bool(false)
	}
	out := listV2(t, s.client, input)
	has := len(out.Contents) > 0 && out.Contents[0].Owner != nil
	if has != (name == "test_bucket_list_v2_fetch_owner_not_empty") {
		t.Fatalf("owner=%#v", out.Contents[0].Owner)
	}
}

func testListV2Max(t *testing.T, name string) {
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	input := &s3.ListObjectsV2Input{Bucket: aws.String(b)}
	if name == "test_bucket_list_v2_max_keys_one" {
		input.MaxKeys = aws.Int32(1)
	} else if name == "test_bucket_list_v2_max_keys_zero" {
		input.MaxKeys = aws.Int32(0)
	}
	out := listV2(t, s.client, input)
	switch name {
	case "test_bucket_list_v2_max_keys_one":
		assertStringList(t, listV2Keys(out), keys[:1])
		if !aws.ToBool(out.IsTruncated) {
			t.Fatal("not truncated")
		}
		out = listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), StartAfter: aws.String(keys[0])})
		assertStringList(t, listV2Keys(out), keys[1:])
	case "test_bucket_list_v2_max_keys_zero":
		if len(out.Contents) != 0 || aws.ToBool(out.IsTruncated) {
			t.Fatalf("contents=%v truncated=%v", out.Contents, out.IsTruncated)
		}
	default:
		assertStringList(t, listV2Keys(out), keys)
		if aws.ToInt32(out.MaxKeys) != 1000 {
			t.Fatalf("MaxKeys=%v", out.MaxKeys)
		}
	}
}

func testListV2Token(t *testing.T, name string) {
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	first := &s3.ListObjectsV2Input{Bucket: aws.String(b), MaxKeys: aws.Int32(1)}
	want := []string{"baz", "foo", "quxx"}
	if name == "test_bucket_list_v2_both_continuation_token_start_after" {
		first.StartAfter = aws.String("bar")
		want = []string{"foo", "quxx"}
	}
	one := listV2(t, s.client, first)
	if one.NextContinuationToken == nil {
		t.Fatal("missing token")
	}
	two := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), ContinuationToken: one.NextContinuationToken, StartAfter: first.StartAfter})
	assertStringList(t, listV2Keys(two), want)
	if aws.ToString(two.ContinuationToken) != aws.ToString(one.NextContinuationToken) || aws.ToBool(two.IsTruncated) {
		t.Fatalf("token=%q truncated=%v", aws.ToString(two.ContinuationToken), two.IsTruncated)
	}
}

func testListV2StartAfter(t *testing.T, name string) {
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	start, want := "\n", keys
	if name == "test_bucket_list_v2_start_after_not_in_list" {
		start, want = "blah", []string{"foo", "quxx"}
	} else if name == "test_bucket_list_v2_start_after_after_list" {
		start, want = "zzz", []string{}
	}
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), StartAfter: aws.String(start)})
	assertStringList(t, listV2Keys(out), want)
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("truncated")
	}
	if name != "test_bucket_list_v2_start_after_unreadable" && aws.ToString(out.StartAfter) != start {
		t.Fatalf("StartAfter=%q", aws.ToString(out.StartAfter))
	}
}

func testListV2Anonymous(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	if name == "test_bucket_list_v2_objects_anonymous" {
		b = ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
		_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(b), ACL: types.BucketCannedACLPublicRead})
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := anonymousClient(s).ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if name == "test_bucket_list_v2_objects_anonymous_fail" {
		assertS3Error(t, err, 403, "AccessDenied")
	} else if err != nil {
		t.Fatal(err)
	}
}

func testListV2Filtering(t *testing.T) {
	s, b := listFixture(t, []string{"test1/f1", "test2/f2", "test3", "test4/f3", "testF4"})
	input := &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("/"), MaxKeys: aws.Int32(3)}
	one := listV2(t, s.client, input)
	assertStringList(t, listV2Keys(one), []string{"test3"})
	assertStringList(t, listV2Prefixes(one), []string{"test1/", "test2/"})
	if !aws.ToBool(one.IsTruncated) || aws.ToInt32(one.KeyCount) != 3 || one.NextContinuationToken == nil {
		t.Fatalf("first=%#v", one)
	}
	input.ContinuationToken = one.NextContinuationToken
	two := listV2(t, s.client, input)
	if aws.ToBool(two.IsTruncated) {
		t.Fatal("second page truncated")
	}
}

func testListV2Versioning(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	keys := []string{"aaa", "bbb", "ccc"}
	for _, key := range keys {
		for i := 0; i < 3; i++ {
			put(t, s, b, key, key, nil)
		}
	}
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	assertStringList(t, listV2Keys(out), keys)
}
