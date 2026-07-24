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

func TestListObjectsVersions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인
		{"test_bucket_list_versions_many", testVersionsMany},
		// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
		{"test_bucket_list_versions_delimiter_basic", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_basic") }},
		// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
		{"test_bucket_list_versions_encoding_basic", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_encoding_basic") }},
		// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
		{"test_bucket_list_versions_delimiter_prefix", func(t *testing.T) { testVersionsDelimiterPages(t, "test_bucket_list_versions_delimiter_prefix") }},
		// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
		{"test_bucket_list_versions_delimiter_prefix_ends_with_delimiter", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_prefix_ends_with_delimiter") }},
		// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
		{"test_bucket_list_versions_delimiter_alt", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_alt") }},
		// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
		{"test_bucket_list_versions_delimiter_prefix_underscore", func(t *testing.T) { testVersionsDelimiterPages(t, "test_bucket_list_versions_delimiter_prefix_underscore") }},
		// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
		{"test_bucket_list_versions_delimiter_percentage", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_percentage") }},
		// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
		{"test_bucket_list_versions_delimiter_whitespace", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_whitespace") }},
		// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
		{"test_bucket_list_versions_delimiter_dot", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_dot") }},
		// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
		{"test_bucket_list_versions_delimiter_unreadable", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_unreadable") }},
		// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
		{"test_bucket_list_versions_delimiter_empty", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_empty") }},
		// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
		{"test_bucket_list_versions_delimiter_none", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_none") }},
		// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
		{"test_bucket_list_versions_delimiter_not_exist", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_delimiter_not_exist") }},
		// 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
		{"test_bucket_list_versions_delimiter_not_skip_special", testVersionsDelimiterBoundary},
		// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
		{"test_bucket_list_versions_prefix_basic", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_basic") }},
		// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
		{"test_bucket_list_versions_prefix_alt", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_alt") }},
		// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
		{"test_bucket_list_versions_prefix_empty", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_empty") }},
		// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
		{"test_bucket_list_versions_prefix_none", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_none") }},
		// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
		{"test_bucket_list_versions_prefix_not_exist", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_not_exist") }},
		// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
		{"test_bucket_list_versions_prefix_unreadable", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_unreadable") }},
		// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
		{"test_bucket_list_versions_prefix_delimiter_basic", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_basic") }},
		// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
		{"test_bucket_list_versions_prefix_delimiter_alt", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_alt") }},
		// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
		{"test_bucket_list_versions_prefix_delimiter_prefix_not_exist", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_prefix_not_exist") }},
		// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
		{"test_bucket_list_versions_prefix_delimiter_delimiter_not_exist", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_delimiter_not_exist") }},
		// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
		{"test_bucket_list_versions_prefix_delimiter_prefix_delimiter_not_exist", func(t *testing.T) { testVersionsSimple(t, "test_bucket_list_versions_prefix_delimiter_prefix_delimiter_not_exist") }},
		// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
		{"test_bucket_list_versions_max_keys_one", func(t *testing.T) { testVersionsMax(t, "test_bucket_list_versions_max_keys_one") }},
		// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
		{"test_bucket_list_versions_max_keys_zero", func(t *testing.T) { testVersionsMax(t, "test_bucket_list_versions_max_keys_zero") }},
		// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
		{"test_bucket_list_versions_max_keys_none", func(t *testing.T) { testVersionsMax(t, "test_bucket_list_versions_max_keys_none") }},
		// 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
		{"test_bucket_list_versions_marker_none", func(t *testing.T) { testVersionsMarker(t, "test_bucket_list_versions_marker_none") }},
		// 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
		{"test_bucket_list_versions_marker_empty", func(t *testing.T) { testVersionsMarker(t, "test_bucket_list_versions_marker_empty") }},
		// 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
		{"test_bucket_list_versions_marker_unreadable", func(t *testing.T) { testVersionsMarker(t, "test_bucket_list_versions_marker_unreadable") }},
		// [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
		{"test_bucket_list_versions_marker_not_in_list", func(t *testing.T) { testVersionsMarker(t, "test_bucket_list_versions_marker_not_in_list") }},
		// [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
		{"test_bucket_list_versions_marker_after_list", func(t *testing.T) { testVersionsMarker(t, "test_bucket_list_versions_marker_after_list") }},
		// ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
		{"test_bucket_list_versions_return_data", testVersionsReturnData},
		// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
		{"test_bucket_list_versions_objects_anonymous", func(t *testing.T) { testVersionsAnonymous(t, "test_bucket_list_versions_objects_anonymous") }},
		// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
		{"test_bucket_list_versions_objects_anonymous_fail", func(t *testing.T) { testVersionsAnonymous(t, "test_bucket_list_versions_objects_anonymous_fail") }},
		// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
		{"test_bucket_list_versions_not_exist", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
			assertS3Error(t, err, 404, "NoSuchBucket")
		}},
		// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
		{"test_versioning_bucket_list_filtering_all", testVersionsFiltering},
		// 버전 목록이 VersionId 기준으로 올바르게 정렬되어 반환되는지 확인
		{"test_versioning_obj_list_marker", testVersionsOrder},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
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
	if strings.HasSuffix(name, "anonymous") {
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
