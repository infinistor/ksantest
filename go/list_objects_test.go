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
func TestBucketListMany(t *testing.T) {
	t.Parallel()
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

// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
func TestBucketListDelimiterBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"}
	wantKeys := []string{"asdf"}
	wantPrefixes := []string{"foo/", "quux/"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("/")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "/")
	}
}

// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
func TestBucketListEncodingBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"}
	wantKeys := []string{"asdf+b"}
	wantPrefixes := []string{"foo+1/", "foo/", "quux ab/"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("/"), EncodingType: types.EncodingTypeUrl})
	gotKeys, gotPrefixes := listKeys(out, true), listPrefixes(out, true)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "/")
	}
}

// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
func TestBucketListDelimiterPrefix(t *testing.T) {
	t.Parallel()
	keys := []string{"asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"}
	object, prefixOne, prefixTwo := "asdf", "boo/", "cquux/"
	nestedObject, nestedPrefix := "boo/bar", "boo/baz/"
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

// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
func TestBucketListDelimiterPrefixEndsWithDelimiter(t *testing.T) {
	t.Parallel()
	keys := []string{"asdf/"}
	wantKeys := []string{"asdf/"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("/"), Prefix: aws.String("asdf/")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "/")
	}
	if aws.ToString(out.Prefix) != "asdf/" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "asdf/")
	}
}

// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
func TestBucketListDelimiterAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"ba", "ca"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("a")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "a" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "a")
	}
}

// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
func TestBucketListDelimiterPrefixUnderscore(t *testing.T) {
	t.Parallel()
	keys := []string{"Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"}
	object, prefixOne, prefixTwo := "Obj1_", "Under1/", "Under2/"
	nestedObject, nestedPrefix := "Under1/bar", "Under1/baz/"
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

// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
func TestBucketListDelimiterPercentage(t *testing.T) {
	t.Parallel()
	keys := []string{"b%ar", "b%az", "c%ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b%", "c%"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("%")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "%" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "%")
	}
}

// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
func TestBucketListDelimiterWhitespace(t *testing.T) {
	t.Parallel()
	keys := []string{"b ar", "b az", "c ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b ", "c "}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String(" ")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != " " {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), " ")
	}
}

// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
func TestBucketListDelimiterDot(t *testing.T) {
	t.Parallel()
	keys := []string{"b.ar", "b.az", "c.ab", "foo"}
	wantKeys := []string{"foo"}
	wantPrefixes := []string{"b.", "c."}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String(".")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "." {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), ".")
	}
}

// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
func TestBucketListDelimiterUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("\n")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "\n" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "\n")
	}
}

// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
func TestBucketListDelimiterEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "")
	}
}

// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
func TestBucketListDelimiterNone(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
}

// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
func TestBucketListDelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "cab", "foo"}
	wantKeys := []string{"bar", "baz", "cab", "foo"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Delimiter: aws.String("/")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "/")
	}
}

// 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
func TestBucketListDelimiterNotSkipSpecial(t *testing.T) {
	t.Parallel()
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

// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
func TestBucketListPrefixBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("foo/")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Prefix) != "foo/" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "foo/")
	}
}

// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
func TestBucketListPrefixAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo"}
	wantKeys := []string{"bar", "baz"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("ba")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Prefix) != "ba" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "ba")
	}
}

// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
func TestBucketListPrefixEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz", "quux"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Prefix) != "" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "")
	}
}

// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
func TestBucketListPrefixNone(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{"foo/bar", "foo/baz", "quux"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
}

// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
func TestBucketListPrefixNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("d")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Prefix) != "d" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "d")
	}
}

// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
func TestBucketListPrefixUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz", "quux"}
	wantKeys := []string{}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("\n")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Prefix) != "\n" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "\n")
	}
}

// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
func TestBucketListPrefixDelimiterBasic(t *testing.T) {
	t.Parallel()
	keys := []string{"foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"}
	wantKeys := []string{"foo/bar"}
	wantPrefixes := []string{"foo/baz/"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("foo/"), Delimiter: aws.String("/")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "/" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "/")
	}
	if aws.ToString(out.Prefix) != "foo/" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "foo/")
	}
}

// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
func TestBucketListPrefixDelimiterAlt(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "bazar", "cab", "foo"}
	wantKeys := []string{"bar"}
	wantPrefixes := []string{"baza"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("ba"), Delimiter: aws.String("a")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "a" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "a")
	}
	if aws.ToString(out.Prefix) != "ba" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "ba")
	}
}

// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
func TestBucketListPrefixDelimiterPrefixNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/r", "b/a/c", "b/a/g", "g"}
	wantKeys := []string{}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("/"), Delimiter: aws.String("d")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "d" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "d")
	}
	if aws.ToString(out.Prefix) != "/" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "/")
	}
}

// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
func TestBucketListPrefixDelimiterDelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/c", "b/a/g", "b/a/r", "g"}
	wantKeys := []string{"b/a/c", "b/a/g", "b/a/r"}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("b"), Delimiter: aws.String("z")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "z" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "z")
	}
	if aws.ToString(out.Prefix) != "b" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "b")
	}
}

// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
func TestBucketListPrefixDelimiterPrefixDelimiterNotExist(t *testing.T) {
	t.Parallel()
	keys := []string{"b/a/r", "b/a/c", "b/a/g", "g"}
	wantKeys := []string{}
	wantPrefixes := []string{}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("y"), Delimiter: aws.String("z")})
	gotKeys, gotPrefixes := listKeys(out, false), listPrefixes(out, false)
	sort.Strings(gotKeys)
	sort.Strings(gotPrefixes)
	sort.Strings(wantKeys)
	sort.Strings(wantPrefixes)
	assertStringList(t, gotKeys, wantKeys)
	assertStringList(t, gotPrefixes, wantPrefixes)
	if aws.ToString(out.Delimiter) != "z" {
		t.Fatalf("delimiter=%q want=%q", aws.ToString(out.Delimiter), "z")
	}
	if aws.ToString(out.Prefix) != "y" {
		t.Fatalf("prefix=%q want=%q", aws.ToString(out.Prefix), "y")
	}
}

// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
func TestBucketListMaxKeysOne(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), MaxKeys: aws.Int32(1)})
	assertStringList(t, listKeys(out, false), keys[:1])
	if !aws.ToBool(out.IsTruncated) {
		t.Fatal("not truncated")
	}
	next := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String(keys[0])})
	assertStringList(t, listKeys(next, false), keys[1:])
}

// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
func TestBucketListMaxKeysZero(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), MaxKeys: aws.Int32(0)})
	if len(out.Contents) != 0 || aws.ToBool(out.IsTruncated) {
		t.Fatalf("contents=%v truncated=%v", out.Contents, aws.ToBool(out.IsTruncated))
	}
}

// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
func TestBucketListMaxKeysNone(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	assertStringList(t, listKeys(out, false), keys)
	if aws.ToInt32(out.MaxKeys) != 1000 || aws.ToBool(out.IsTruncated) {
		t.Fatalf("max=%v truncated=%v", out.MaxKeys, aws.ToBool(out.IsTruncated))
	}
}

// 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
func TestBucketListMarkerNone(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String("")})
	assertStringList(t, listKeys(out, false), keys)
	if aws.ToString(out.NextMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextMarker), aws.ToBool(out.IsTruncated))
	}
}

// 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
func TestBucketListMarkerEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String("")})
	assertStringList(t, listKeys(out, false), keys)
	if aws.ToString(out.NextMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextMarker), aws.ToBool(out.IsTruncated))
	}
}

// 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
func TestBucketListMarkerUnreadable(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	marker := "\n"
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String(marker)})
	assertStringList(t, listKeys(out, false), keys)
	if aws.ToString(out.NextMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextMarker), aws.ToBool(out.IsTruncated))
	}
	if aws.ToString(out.Marker) != marker {
		t.Fatalf("marker=%q want=%q", aws.ToString(out.Marker), marker)
	}
}

// [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
func TestBucketListMarkerNotInList(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	marker := "blah"
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String(marker)})
	assertStringList(t, listKeys(out, false), []string{"foo", "quxx"})
	if aws.ToString(out.NextMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextMarker), aws.ToBool(out.IsTruncated))
	}
	if aws.ToString(out.Marker) != marker {
		t.Fatalf("marker=%q want=%q", aws.ToString(out.Marker), marker)
	}
}

// [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
func TestBucketListMarkerAfterList(t *testing.T) {
	t.Parallel()
	keys := []string{"bar", "baz", "foo", "quxx"}
	s, bucket := listFixture(t, keys)
	marker := "zzz"
	out := listV1(t, s.client, &s3.ListObjectsInput{Bucket: aws.String(bucket), Marker: aws.String(marker)})
	assertStringList(t, listKeys(out, false), []string{})
	if aws.ToString(out.NextMarker) != "" || aws.ToBool(out.IsTruncated) {
		t.Fatalf("next=%q truncated=%v", aws.ToString(out.NextMarker), aws.ToBool(out.IsTruncated))
	}
	if aws.ToString(out.Marker) != marker {
		t.Fatalf("marker=%q want=%q", aws.ToString(out.Marker), marker)
	}
}

// ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
func TestBucketListReturnData(t *testing.T) {
	t.Parallel()
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

// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
func TestBucketListObjectsAnonymous(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	_, err = anonymousClient(s).ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
}

// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
func TestBucketListObjectsAnonymousFail(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t)
	_, err := anonymousClient(s).ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
func TestBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
func TestBucketListFilteringAll(t *testing.T) {
	t.Parallel()
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

// versioning 활성화 버킷에서 오브젝트 목록을 가져올때 버전정보가 포함되어 있는지 확인
func TestBucketListVersioning(t *testing.T) {
	t.Parallel()
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
