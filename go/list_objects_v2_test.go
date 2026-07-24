package s3tests

import (
	"context"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestListObjectsV2(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_many", testListV2Many},
		// ListObjectsV2로 오브젝트 목록을 가져올때 Key Count 값을 올바르게 가져오는지 확인
		{"test_basic_key_count", func(t *testing.T) {
			s, b := listFixture(t, []string{"0", "1", "2", "3", "4"})
			out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
			if aws.ToInt32(out.KeyCount) != 5 {
				t.Fatalf("KeyCount=%v", out.KeyCount)
			}
		}},
		// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_basic", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_basic") }},
		// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_encoding_basic", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_encoding_basic") }},
		// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_prefix", func(t *testing.T) { testListV2DelimiterPages(t, "test_bucket_list_v2_delimiter_prefix") }},
		// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_prefix_ends_with_delimiter", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_prefix_ends_with_delimiter") }},
		// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_alt", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_alt") }},
		// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_prefix_underscore", func(t *testing.T) { testListV2DelimiterPages(t, "test_bucket_list_v2_delimiter_prefix_underscore") }},
		// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_percentage", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_percentage") }},
		// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_whitespace", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_whitespace") }},
		// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_dot", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_dot") }},
		// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_unreadable", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_unreadable") }},
		// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_empty", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_empty") }},
		// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_none", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_none") }},
		// [권한정보를 가져오도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_fetch_owner_not_empty", func(t *testing.T) { testListV2Owner(t, "test_bucket_list_v2_fetch_owner_not_empty") }},
		// [default = 권한정보를 가져오지 않음] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_fetch_owner_default_empty", func(t *testing.T) { testListV2Owner(t, "test_bucket_list_v2_fetch_owner_default_empty") }},
		// [권한정보를 가져오지 않도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_fetch_owner_empty", func(t *testing.T) { testListV2Owner(t, "test_bucket_list_v2_fetch_owner_empty") }},
		// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_delimiter_not_exist", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_delimiter_not_exist") }},
		// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_basic", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_basic") }},
		// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_alt", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_alt") }},
		// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_empty", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_empty") }},
		// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_none", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_none") }},
		// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_not_exist", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_not_exist") }},
		// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_unreadable", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_unreadable") }},
		// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_delimiter_basic", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_basic") }},
		// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_delimiter_alt", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_alt") }},
		// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_delimiter_prefix_not_exist", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_prefix_not_exist") }},
		// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_delimiter_delimiter_not_exist", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_delimiter_not_exist") }},
		// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_prefix_delimiter_prefix_delimiter_not_exist", func(t *testing.T) { testListV2Simple(t, "test_bucket_list_v2_prefix_delimiter_prefix_delimiter_not_exist") }},
		// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_max_keys_one", func(t *testing.T) { testListV2Max(t, "test_bucket_list_v2_max_keys_one") }},
		// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_max_keys_zero", func(t *testing.T) { testListV2Max(t, "test_bucket_list_v2_max_keys_zero") }},
		// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_max_keys_none", func(t *testing.T) { testListV2Max(t, "test_bucket_list_v2_max_keys_none") }},
		// 오브젝트 목록을 가져올때 다음 토큰값을 올바르게 가져오는지 확인
		{"test_bucket_list_v2_continuation_token", func(t *testing.T) { testListV2Token(t, "test_bucket_list_v2_continuation_token") }},
		// 오브젝트 목록을 가져올때 StartAfter와 토큰이 재대로 동작하는지 확인
		{"test_bucket_list_v2_both_continuation_token_start_after", func(t *testing.T) { testListV2Token(t, "test_bucket_list_v2_both_continuation_token_start_after") }},
		// startAfter에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
		{"test_bucket_list_v2_start_after_unreadable", func(t *testing.T) { testListV2StartAfter(t, "test_bucket_list_v2_start_after_unreadable") }},
		// [startAfter와 일치하는 오브젝트가 존재하지 않는 환경 해당 startAfter보다 정렬순서가 낮은 오브젝트는 존재하는 환경] startAfter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
		{"test_bucket_list_v2_start_after_not_in_list", func(t *testing.T) { testListV2StartAfter(t, "test_bucket_list_v2_start_after_not_in_list") }},
		// [startAfter와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] startAfter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
		{"test_bucket_list_v2_start_after_after_list", func(t *testing.T) { testListV2StartAfter(t, "test_bucket_list_v2_start_after_after_list") }},
		// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_objects_anonymous", func(t *testing.T) { testListV2Anonymous(t, "test_bucket_list_v2_objects_anonymous") }},
		// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인(ListObjectsV2)
		{"test_bucket_list_v2_objects_anonymous_fail", func(t *testing.T) { testListV2Anonymous(t, "test_bucket_list_v2_objects_anonymous_fail") }},
		// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인(ListObjectsV2)
		{"test_bucket_v2_not_exist", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
			assertS3Error(t, err, 404, "NoSuchBucket")
		}},
		// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
		{"test_bucket_list_v2_filtering_all", testListV2Filtering},
		// versioning 활성화 버킷에서 오브젝트 목록을 가져올때 버전정보가 포함되어 있는지 확인
		{"test_bucket_list_v2_versioning", testListV2Versioning},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
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
