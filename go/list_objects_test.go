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

func TestListObjects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인
		{"test_bucket_list_many", testListObjectsMany},
		// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
		{"test_bucket_list_delimiter_basic", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_basic") }},
		// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
		{"test_bucket_list_encoding_basic", func(t *testing.T) { testListSimple(t, "test_bucket_list_encoding_basic") }},
		// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
		{"test_bucket_list_delimiter_prefix", func(t *testing.T) { testListDelimiterPagination(t, "test_bucket_list_delimiter_prefix") }},
		// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
		{"test_bucket_list_delimiter_prefix_ends_with_delimiter", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_prefix_ends_with_delimiter") }},
		// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
		{"test_bucket_list_delimiter_alt", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_alt") }},
		// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
		{"test_bucket_list_delimiter_prefix_underscore", func(t *testing.T) { testListDelimiterPagination(t, "test_bucket_list_delimiter_prefix_underscore") }},
		// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
		{"test_bucket_list_delimiter_percentage", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_percentage") }},
		// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
		{"test_bucket_list_delimiter_whitespace", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_whitespace") }},
		// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
		{"test_bucket_list_delimiter_dot", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_dot") }},
		// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
		{"test_bucket_list_delimiter_unreadable", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_unreadable") }},
		// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
		{"test_bucket_list_delimiter_empty", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_empty") }},
		// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
		{"test_bucket_list_delimiter_none", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_none") }},
		// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
		{"test_bucket_list_delimiter_not_exist", func(t *testing.T) { testListSimple(t, "test_bucket_list_delimiter_not_exist") }},
		// 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
		{"test_bucket_list_delimiter_not_skip_special", testListDelimiterBoundary},
		// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
		{"test_bucket_list_prefix_basic", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_basic") }},
		// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
		{"test_bucket_list_prefix_alt", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_alt") }},
		// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
		{"test_bucket_list_prefix_empty", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_empty") }},
		// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
		{"test_bucket_list_prefix_none", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_none") }},
		// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
		{"test_bucket_list_prefix_not_exist", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_not_exist") }},
		// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
		{"test_bucket_list_prefix_unreadable", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_unreadable") }},
		// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
		{"test_bucket_list_prefix_delimiter_basic", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_delimiter_basic") }},
		// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
		{"test_bucket_list_prefix_delimiter_alt", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_delimiter_alt") }},
		// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
		{"test_bucket_list_prefix_delimiter_prefix_not_exist", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_delimiter_prefix_not_exist") }},
		// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
		{"test_bucket_list_prefix_delimiter_delimiter_not_exist", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_delimiter_delimiter_not_exist") }},
		// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
		{"test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist", func(t *testing.T) { testListSimple(t, "test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist") }},
		// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
		{"test_bucket_list_max_keys_one", func(t *testing.T) { testListMaxKeys(t, "test_bucket_list_max_keys_one") }},
		// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
		{"test_bucket_list_max_keys_zero", func(t *testing.T) { testListMaxKeys(t, "test_bucket_list_max_keys_zero") }},
		// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
		{"test_bucket_list_max_keys_none", func(t *testing.T) { testListMaxKeys(t, "test_bucket_list_max_keys_none") }},
		// 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
		{"test_bucket_list_marker_none", func(t *testing.T) { testListMarker(t, "test_bucket_list_marker_none") }},
		// 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
		{"test_bucket_list_marker_empty", func(t *testing.T) { testListMarker(t, "test_bucket_list_marker_empty") }},
		// 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
		{"test_bucket_list_marker_unreadable", func(t *testing.T) { testListMarker(t, "test_bucket_list_marker_unreadable") }},
		// [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
		{"test_bucket_list_marker_not_in_list", func(t *testing.T) { testListMarker(t, "test_bucket_list_marker_not_in_list") }},
		// [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
		{"test_bucket_list_marker_after_list", func(t *testing.T) { testListMarker(t, "test_bucket_list_marker_after_list") }},
		// ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
		{"test_bucket_list_return_data", testListReturnData},
		// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
		{"test_bucket_list_objects_anonymous", func(t *testing.T) { testListAnonymous(t, "test_bucket_list_objects_anonymous") }},
		// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
		{"test_bucket_list_objects_anonymous_fail", func(t *testing.T) { testListAnonymous(t, "test_bucket_list_objects_anonymous_fail") }},
		// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
		{"test_bucket_not_exist", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
			assertS3Error(t, err, 404, "NoSuchBucket")
		}},
		// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
		{"test_bucket_list_filtering_all", testListFilteringAll},
		// versioning 활성화 버킷에서 오브젝트 목록을 가져올때 버전정보가 포함되어 있는지 확인
		{"test_bucket_list_versioning", testListVersioning},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
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
