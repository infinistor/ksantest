package s3tests

import (
	"context"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인(ListObjectsV2)
func TestBucketListV2Many(t *testing.T) {
	t.Parallel()
	s, b := listFixture(t, []string{"foo", "bar", "baz"}, 1)
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

// ListObjectsV2로 오브젝트 목록을 가져올때 Key Count 값을 올바르게 가져오는지 확인
func TestBasicKeyCount(t *testing.T) {
	t.Parallel()

	s, b := listFixture(t, []string{"0", "1", "2", "3", "4"}, 2)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if aws.ToInt32(out.KeyCount) != 5 {
		t.Fatalf("KeyCount=%v", out.KeyCount)
	}
}

// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"}
	wantKeys := []string{"asdf"}
	wantPrefixes := []string{"foo/", "quux/"}
	s, b := listFixture(t, keys, 3)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("/")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인(ListObjectsV2)
func TestBucketListV2EncodingBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"}
	wantKeys := []string{"asdf%2Bb"}
	wantPrefixes := []string{"foo%2B1/", "foo/", "quux+ab/"}
	s, b := listFixture(t, keys, 4)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("/"), EncodingType: types.EncodingTypeUrl})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterPrefix(t *testing.T) {
	t.Parallel()
	keys := []string{"asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"}
	object, p1, p2, nested, nestedP := "asdf", "boo/", "cquux/", "boo/bar", "boo/baz/"
	s, b := listFixture(t, keys, 5)
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

// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterPrefixEndsWithDelimiter(t *testing.T) {
	t.Parallel()
	keys := []string{"asdf/"}
	wantKeys := []string{"asdf/"}
	s, b := listFixture(t, keys, 6)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("/"), Prefix: aws.String("asdf/")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "asdf/" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"ba", "ca"}
	s, b := listFixture(t, keys, 7)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("a")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "a" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterPrefixUnderscore(t *testing.T) {
	t.Parallel()
	keys := []string{"Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"}
	object, p1, p2, nested, nestedP := "Obj1_", "Under1/", "Under2/", "Under1/bar", "Under1/baz/"
	s, b := listFixture(t, keys, 8)
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

// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterPercentage(t *testing.T) {
	t.Parallel()
	keys := []string{"b%ar", "b%az", "c%ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b%", "c%"}
	s, b := listFixture(t, keys, 9)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("%")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "%" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterWhitespace(t *testing.T) {
	t.Parallel()
	keys := []string{"b ar", "b az", "c ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b ", "c "}
	s, b := listFixture(t, keys, 10)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String(" ")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != " " {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterDot(t *testing.T) {
	t.Parallel()
	keys := []string{"b.ar", "b.az", "c.ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b.", "c."}
	s, b := listFixture(t, keys, 11)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String(".")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "." {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	s, b := listFixture(t, keys, 12)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("\n")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Delimiter) != "\n" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	s, b := listFixture(t, keys, 13)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Delimiter) != "" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterNone(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	s, b := listFixture(t, keys, 14)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
}

// [권한정보를 가져오도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
func TestBucketListV2FetchOwnerNotEmpty(t *testing.T) {
	t.Parallel()
	s, b := listFixture(t, []string{"foo/bar", "foo/baz", "quux"}, 15)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), FetchOwner: aws.Bool(true)})
	has := len(out.Contents) > 0 && out.Contents[0].Owner != nil
	if !has {
		t.Fatalf("owner=%#v", out.Contents[0].Owner)
	}
}

// [default = 권한정보를 가져오지 않음] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
func TestBucketListV2FetchOwnerDefaultEmpty(t *testing.T) {
	t.Parallel()
	s, b := listFixture(t, []string{"foo/bar", "foo/baz", "quux"}, 16)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	has := len(out.Contents) > 0 && out.Contents[0].Owner != nil
	if has {
		t.Fatalf("owner=%#v", out.Contents[0].Owner)
	}
}

// [권한정보를 가져오지 않도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
func TestBucketListV2FetchOwnerEmpty(t *testing.T) {
	t.Parallel()
	s, b := listFixture(t, []string{"foo/bar", "foo/baz", "quux"}, 17)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), FetchOwner: aws.Bool(false)})
	has := len(out.Contents) > 0 && out.Contents[0].Owner != nil
	if has {
		t.Fatalf("owner=%#v", out.Contents[0].Owner)
	}
}

// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
func TestBucketListV2DelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	s, b := listFixture(t, keys, 18)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("/")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인(ListObjectsV2)
func TestBucketListV2PrefixBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz"}
	s, b := listFixture(t, keys, 19)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("foo/")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Prefix) != "foo/" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인(ListObjectsV2)
func TestBucketListV2PrefixAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo"}
	wantKeys := []string{"bar", "baz"}
	s, b := listFixture(t, keys, 20)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("ba")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Prefix) != "ba" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
func TestBucketListV2PrefixEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz", "quux"}
	s, b := listFixture(t, keys, 21)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Prefix) != "" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
func TestBucketListV2PrefixNone(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz", "quux"}
	s, b := listFixture(t, keys, 22)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
}

// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
func TestBucketListV2PrefixNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{}
	s, b := listFixture(t, keys, 23)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("d")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Prefix) != "d" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
func TestBucketListV2PrefixUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{}
	s, b := listFixture(t, keys, 24)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("\n")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Prefix) != "\n" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
func TestBucketListV2PrefixDelimiterBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"}
	wantKeys := []string{"foo/bar"}
	wantPrefixes := []string{"foo/baz/"}
	s, b := listFixture(t, keys, 25)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("foo/"), Delimiter: aws.String("/")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "foo/" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
func TestBucketListV2PrefixDelimiterAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "bazar", "cab", "foo"}
	wantKeys := []string{"bar"}
	wantPrefixes := []string{"baza"}
	s, b := listFixture(t, keys, 26)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("ba"), Delimiter: aws.String("a")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "a" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "ba" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
func TestBucketListV2PrefixDelimiterPrefixNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/r", "b/a/c", "b/a/g", "g"}
	wantKeys := []string{}
	s, b := listFixture(t, keys, 27)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("/"), Delimiter: aws.String("d")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Delimiter) != "d" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "/" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
func TestBucketListV2PrefixDelimiterDelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/c", "b/a/g", "b/a/r", "g"}
	wantKeys := []string{"b/a/c", "b/a/g", "b/a/r"}
	s, b := listFixture(t, keys, 28)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("b"), Delimiter: aws.String("z")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Delimiter) != "z" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "b" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
func TestBucketListV2PrefixDelimiterPrefixDelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/r", "b/a/c", "b/a/g", "g"}
	wantKeys := []string{}
	s, b := listFixture(t, keys, 29)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("y"), Delimiter: aws.String("z")})
	gotKeys, gotPrefixes := listV2Keys(out), listV2Prefixes(out)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, nil)
	if aws.ToString(out.Delimiter) != "z" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "y" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
func TestBucketListV2MaxKeysOne(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys, 30)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), MaxKeys: aws.Int32(1)})
	assertStringList(t, listV2Keys(out), keys[:1])
	if !aws.ToBool(out.IsTruncated) {
		t.Fatal("not truncated")
	}
	out = listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), StartAfter: aws.String(keys[0])})
	assertStringList(t, listV2Keys(out), keys[1:])
}

// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인(ListObjectsV2)
func TestBucketListV2MaxKeysZero(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys, 31)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), MaxKeys: aws.Int32(0)})
	if len(out.Contents) != 0 || aws.ToBool(out.IsTruncated) {
		t.Fatalf("contents=%v truncated=%v", out.Contents, out.IsTruncated)
	}
}

// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
func TestBucketListV2MaxKeysNone(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys, 32)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	assertStringList(t, listV2Keys(out), keys)
	if aws.ToInt32(out.MaxKeys) != 1000 {
		t.Fatalf("MaxKeys=%v", out.MaxKeys)
	}
}

// 오브젝트 목록을 가져올때 다음 토큰값을 올바르게 가져오는지 확인
func TestBucketListV2ContinuationToken(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys, 33)
	one := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), MaxKeys: aws.Int32(1)})
	if one.NextContinuationToken == nil {
		t.Fatal("missing token")
	}
	two := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), ContinuationToken: one.NextContinuationToken})
	assertStringList(t, listV2Keys(two), []string{"baz", "foo", "quxx"})
	if aws.ToString(two.ContinuationToken) != aws.ToString(one.NextContinuationToken) || aws.ToBool(two.IsTruncated) {
		t.Fatalf("token=%q truncated=%v", aws.ToString(two.ContinuationToken), two.IsTruncated)
	}
}

// 오브젝트 목록을 가져올때 StartAfter와 토큰이 재대로 동작하는지 확인
func TestBucketListV2BothContinuationTokenStartAfter(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys, 34)
	one := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), MaxKeys: aws.Int32(1), StartAfter: aws.String("bar")})
	if one.NextContinuationToken == nil {
		t.Fatal("missing token")
	}
	two := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), ContinuationToken: one.NextContinuationToken, StartAfter: aws.String("bar")})
	assertStringList(t, listV2Keys(two), []string{"foo", "quxx"})
	if aws.ToString(two.ContinuationToken) != aws.ToString(one.NextContinuationToken) || aws.ToBool(two.IsTruncated) {
		t.Fatalf("token=%q truncated=%v", aws.ToString(two.ContinuationToken), two.IsTruncated)
	}
}

// startAfter에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
func TestBucketListV2StartAfterUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys, 35)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), StartAfter: aws.String("\n")})
	assertStringList(t, listV2Keys(out), keys)
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("truncated")
	}
}

// [startAfter와 일치하는 오브젝트가 존재하지 않는 환경 해당 startAfter보다 정렬순서가 낮은 오브젝트는 존재하는 환경] startAfter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
func TestBucketListV2StartAfterNotInList(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys, 36)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), StartAfter: aws.String("blah")})
	assertStringList(t, listV2Keys(out), []string{"foo", "quxx"})
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("truncated")
	}
	if aws.ToString(out.StartAfter) != "blah" {
		t.Fatalf("StartAfter=%q", aws.ToString(out.StartAfter))
	}
}

// [startAfter와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] startAfter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
func TestBucketListV2StartAfterAfterList(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys, 37)
	out := listV2(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b), StartAfter: aws.String("zzz")})
	assertStringList(t, listV2Keys(out), []string{})
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("truncated")
	}
	if aws.ToString(out.StartAfter) != "zzz" {
		t.Fatalf("StartAfter=%q", aws.ToString(out.StartAfter))
	}
}

// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인(ListObjectsV2)
func TestBucketListV2ObjectsAnonymous(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 38)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(b), ACL: types.BucketCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	_, err = anonymousClient(s).ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
}

// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인(ListObjectsV2)
func TestBucketListV2ObjectsAnonymousFail(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 39)
	_, err := anonymousClient(s).ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인(ListObjectsV2)
func TestBucketV2NotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
func TestBucketListV2FilteringAll(t *testing.T) {
	t.Parallel()
	s, b := listFixture(t, []string{"test1/f1", "test2/f2", "test3", "test4/f3", "testF4"}, 41)
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

// versioning 활성화 버킷에서 오브젝트 목록을 가져올때 버전정보가 포함되어 있는지 확인
func TestBucketListV2Versioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 42)
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
