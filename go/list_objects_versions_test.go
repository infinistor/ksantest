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

// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인
func TestBucketListVersionsMany(t *testing.T) {
	t.Parallel()
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

// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
func TestBucketListVersionsDelimiterBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"}
	wantKeys := []string{"asdf"}
	wantPrefixes := []string{"foo/", "quux/"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("/")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
func TestBucketListVersionsEncodingBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"}
	wantKeys := []string{"asdf+b"}
	wantPrefixes := []string{"foo+1/", "foo/", "quux ab/"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("/"), EncodingType: types.EncodingTypeUrl})
	gotK, gotP := versionKeys(out, true), versionPrefixes(out, true)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
func TestBucketListVersionsDelimiterPrefix(t *testing.T) {
	t.Parallel()
	keys := []string{"asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"}
	object, p1, p2, nested, nestedP := "asdf", "boo/", "cquux/", "boo/bar", "boo/baz/"
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

// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
func TestBucketListVersionsDelimiterPrefixEndsWithDelimiter(t *testing.T) {
	t.Parallel()
	keys := []string{"asdf/"}
	wantKeys := []string{"asdf/"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("/"), Prefix: aws.String("asdf/")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "asdf/" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
func TestBucketListVersionsDelimiterAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"ba", "ca"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("a")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, wantPrefixes)
	if aws.ToString(out.Delimiter) != "a" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
func TestBucketListVersionsDelimiterPrefixUnderscore(t *testing.T) {
	t.Parallel()
	keys := []string{"Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"}
	object, p1, p2, nested, nestedP := "Obj1_", "Under1/", "Under2/", "Under1/bar", "Under1/baz/"
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

// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
func TestBucketListVersionsDelimiterPercentage(t *testing.T) {
	t.Parallel()
	keys := []string{"b%ar", "b%az", "c%ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b%", "c%"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("%")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, wantPrefixes)
	if aws.ToString(out.Delimiter) != "%" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
func TestBucketListVersionsDelimiterWhitespace(t *testing.T) {
	t.Parallel()
	keys := []string{"b ar", "b az", "c ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b ", "c "}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String(" ")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, wantPrefixes)
	if aws.ToString(out.Delimiter) != " " {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
func TestBucketListVersionsDelimiterDot(t *testing.T) {
	t.Parallel()
	keys := []string{"b.ar", "b.az", "c.ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b.", "c."}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String(".")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, wantPrefixes)
	if aws.ToString(out.Delimiter) != "." {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
func TestBucketListVersionsDelimiterUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("\n")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Delimiter) != "\n" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
func TestBucketListVersionsDelimiterEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Delimiter) != "" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
func TestBucketListVersionsDelimiterNone(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
}

// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
func TestBucketListVersionsDelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Delimiter: aws.String("/")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
}

// 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
func TestBucketListVersionsDelimiterNotSkipSpecial(t *testing.T) {
	t.Parallel()
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

// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
func TestBucketListVersionsPrefixBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("foo/")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Prefix) != "foo/" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
func TestBucketListVersionsPrefixAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo"}
	wantKeys := []string{"bar", "baz"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("ba")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Prefix) != "ba" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
func TestBucketListVersionsPrefixEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz", "quux"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Prefix) != "" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
func TestBucketListVersionsPrefixNone(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz", "quux"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
}

// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
func TestBucketListVersionsPrefixNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("d")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Prefix) != "d" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
func TestBucketListVersionsPrefixUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("\n")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Prefix) != "\n" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
func TestBucketListVersionsPrefixDelimiterBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"}
	wantKeys := []string{"foo/bar"}
	wantPrefixes := []string{"foo/baz/"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("foo/"), Delimiter: aws.String("/")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "foo/" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
func TestBucketListVersionsPrefixDelimiterAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "bazar", "cab", "foo"}
	wantKeys := []string{"bar"}
	wantPrefixes := []string{"baza"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("ba"), Delimiter: aws.String("a")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, wantPrefixes)
	if aws.ToString(out.Delimiter) != "a" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "ba" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
func TestBucketListVersionsPrefixDelimiterPrefixNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/r", "b/a/c", "b/a/g", "g"}
	wantKeys := []string{}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("/"), Delimiter: aws.String("d")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Delimiter) != "d" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "/" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
func TestBucketListVersionsPrefixDelimiterDelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/c", "b/a/g", "b/a/r", "g"}
	wantKeys := []string{"b/a/c", "b/a/g", "b/a/r"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("b"), Delimiter: aws.String("z")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Delimiter) != "z" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "b" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
func TestBucketListVersionsPrefixDelimiterPrefixDelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/r", "b/a/c", "b/a/g", "g"}
	wantKeys := []string{}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("y"), Delimiter: aws.String("z")})
	gotK, gotP := versionKeys(out, false), versionPrefixes(out, false)
	sort.Strings(gotK)
	sort.Strings(gotP)
	sort.Strings(wantKeys)
	assertStringList(t, gotK, wantKeys)
	assertStringList(t, gotP, nil)
	if aws.ToString(out.Delimiter) != "z" {
		t.Fatalf("delimiter=%q", aws.ToString(out.Delimiter))
	}
	if aws.ToString(out.Prefix) != "y" {
		t.Fatalf("prefix=%q", aws.ToString(out.Prefix))
	}
}

// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
func TestBucketListVersionsMaxKeysOne(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), MaxKeys: aws.Int32(1)})
	assertStringList(t, versionKeys(out, false), keys[:1])
	if !aws.ToBool(out.IsTruncated) {
		t.Fatal("not truncated")
	}
	out = listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String(keys[0])})
	assertStringList(t, versionKeys(out, false), keys[1:])
}

// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
func TestBucketListVersionsMaxKeysZero(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), MaxKeys: aws.Int32(0)})
	if len(out.Versions) != 0 || aws.ToBool(out.IsTruncated) {
		t.Fatalf("versions=%v truncated=%v", out.Versions, out.IsTruncated)
	}
}

// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
func TestBucketListVersionsMaxKeysNone(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	assertStringList(t, versionKeys(out, false), keys)
	if aws.ToInt32(out.MaxKeys) != 1000 {
		t.Fatalf("MaxKeys=%v", out.MaxKeys)
	}
}

// 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
func TestBucketListVersionsMarkerNone(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String("")})
	if out.NextKeyMarker != nil {
		t.Fatalf("next=%q", aws.ToString(out.NextKeyMarker))
	}
}

// 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
func TestBucketListVersionsMarkerEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String("")})
	assertStringList(t, versionKeys(out, false), keys)
	if aws.ToString(out.NextKeyMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextKeyMarker), out.IsTruncated)
	}
}

// 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
func TestBucketListVersionsMarkerUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	marker := "\n"
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String(marker)})
	assertStringList(t, versionKeys(out, false), keys)
	if aws.ToString(out.NextKeyMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextKeyMarker), out.IsTruncated)
	}
	if aws.ToString(out.KeyMarker) != marker {
		t.Fatalf("marker=%q", aws.ToString(out.KeyMarker))
	}
}

// [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
func TestBucketListVersionsMarkerNotInList(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	marker := "blah"
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String(marker)})
	assertStringList(t, versionKeys(out, false), []string{"foo", "quxx"})
	if aws.ToString(out.NextKeyMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextKeyMarker), out.IsTruncated)
	}
	if aws.ToString(out.KeyMarker) != marker {
		t.Fatalf("marker=%q", aws.ToString(out.KeyMarker))
	}
}

// [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
func TestBucketListVersionsMarkerAfterList(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, b := listFixture(t, keys)
	marker := "zzz"
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), KeyMarker: aws.String(marker)})
	assertStringList(t, versionKeys(out, false), []string{})
	if aws.ToString(out.NextKeyMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextKeyMarker), out.IsTruncated)
	}
	if aws.ToString(out.KeyMarker) != marker {
		t.Fatalf("marker=%q", aws.ToString(out.KeyMarker))
	}
}

// ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
func TestBucketListVersionsReturnData(t *testing.T) {
	t.Parallel()
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

// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
func TestBucketListVersionsObjectsAnonymous(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(b), ACL: types.BucketCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	_, err = anonymousClient(s).ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
}

// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
func TestBucketListVersionsObjectsAnonymousFail(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	_, err := anonymousClient(s).ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
func TestBucketListVersionsNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
func TestVersioningBucketListFilteringAll(t *testing.T) {
	t.Parallel()
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

// 버전 목록이 VersionId 기준으로 올바르게 정렬되어 반환되는지 확인
func TestVersioningObjListMarker(t *testing.T) {
	t.Parallel()
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
