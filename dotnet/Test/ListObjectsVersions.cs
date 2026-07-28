/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
using Amazon.S3;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class ListObjectsVersions : TestBase
	{
		public ListObjectsVersions(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 오브젝트 버전 목록을 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMany()
		{
			var bucketName = SetupObjects(["foo", "bar", "baz"]);
			var client = GetClient();

			var response = client.ListVersions(bucketName, maxKeys: 2);
			Assert.Equal(["bar", "baz"], GetKeys(response));
			Assert.Equal(2, response.Versions.Count);
			Assert.True(response.IsTruncated);

			response = client.ListVersions(bucketName, keyMarker: "baz", maxKeys: 2);
			Assert.Equal(["foo"], GetKeys(response));
			Assert.Single(response.Versions);
			Assert.False(response.IsTruncated);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterBasic()
		{
			var bucketName = SetupObjects(["foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"]);
			var client = GetClient();

			string delimiter = "/";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal(delimiter, response.Delimiter);
			Assert.Equal(["asdf"], GetKeys(response));

			var prefixes = response.CommonPrefixes;
			Assert.Equal(2, prefixes.Count);
			Assert.Equal(["foo/", "quux/"], prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Encoding")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 인코딩이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsEncodingBasic()
		{
			var bucketName = SetupObjects(["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"]);
			var client = GetClient();

			string delimiter = "/";

			var response = client.ListVersions(bucketName, delimiter: delimiter, encodingTypeName: "url");
			Assert.Equal(delimiter, response.Delimiter);
			// encodingType=url 응답의 키는 SDK가 디코딩하지 않으므로 직접 디코딩해서 비교한다.
			Assert.Equal(["asdf+b"], GetKeys(response).Select(UrlDecode).ToList());

			var prefixes = response.CommonPrefixes;
			Assert.Equal(3, prefixes.Count);
			Assert.Equal(["foo+1/", "foo/", "quux ab/"], prefixes.Select(UrlDecode).ToList());
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Filtering")]
		[Trait(MainData.Explanation, "조건에 맞는 오브젝트 버전 목록을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterPrefix()
		{
			var bucketName = SetupObjects(["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"]);

			string delimiter = "/";
			string marker = "";
			string prefix = "";

			marker = ValidateListObjectVersions(bucketName, prefix, delimiter, "", 1, true, ["asdf"], EmptyList, "asdf");
			marker = ValidateListObjectVersions(bucketName, prefix, delimiter, marker, 1, true, EmptyList, ["boo/"], "boo/");
			ValidateListObjectVersions(bucketName, prefix, delimiter, marker, 1, false, EmptyList, ["cquux/"], null);

			marker = ValidateListObjectVersions(bucketName, prefix, delimiter, "", 2, true, ["asdf"], ["boo/"], "boo/");
			ValidateListObjectVersions(bucketName, prefix, delimiter, marker, 2, false, EmptyList, ["cquux/"], null);

			prefix = "boo/";

			marker = ValidateListObjectVersions(bucketName, prefix, delimiter, "", 1, true, ["boo/bar"], EmptyList, "boo/bar");
			ValidateListObjectVersions(bucketName, prefix, delimiter, marker, 1, false, EmptyList, ["boo/baz/"], null);

			ValidateListObjectVersions(bucketName, prefix, delimiter, "", 2, false, ["boo/bar"], ["boo/baz/"], null);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Filtering")]
		[Trait(MainData.Explanation, "비어있는 폴더의 오브젝트 버전 목록을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterPrefixEndsWithDelimiter()
		{
			var bucketName = SetupObjects(["asdf/"], body: "");
			ValidateListObjectVersions(bucketName, "asdf/", "/", "", 1000, false, ["asdf/"], EmptyList, null);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterAlt()
		{
			var bucketName = SetupObjects(["bar", "baz", "cab", "foo"]);
			var client = GetClient();

			string delimiter = "a";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			Assert.Equal(["foo"], keys);

			var prefixes = response.CommonPrefixes;
			Assert.Equal(2, prefixes.Count);
			Assert.Equal(["ba", "ca"], prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Filtering")]
		[Trait(MainData.Explanation, "[폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 버전 목록을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterPrefixUnderscore()
		{
			var bucketName = SetupObjects(["Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"]);

			string delimiter = "/";
			string marker = "";
			string prefix = "";

			marker = ValidateListObjectVersions(bucketName, prefix, delimiter, "", 1, true, ["Obj1_"], EmptyList, "Obj1_");
			marker = ValidateListObjectVersions(bucketName, prefix, delimiter, marker, 1, true, EmptyList, ["Under1/"], "Under1/");
			ValidateListObjectVersions(bucketName, prefix, delimiter, marker, 1, false, EmptyList, ["Under2/"], null);

			marker = ValidateListObjectVersions(bucketName, prefix, delimiter, "", 2, true, ["Obj1_"], ["Under1/"], "Under1/");
			ValidateListObjectVersions(bucketName, prefix, delimiter, marker, 2, false, EmptyList, ["Under2/"], null);

			prefix = "Under1/";

			marker = ValidateListObjectVersions(bucketName, prefix, delimiter, "", 1, true, ["Under1/bar"], EmptyList, "Under1/bar");
			ValidateListObjectVersions(bucketName, prefix, delimiter, marker, 1, false, EmptyList, ["Under1/baz/"], null);

			ValidateListObjectVersions(bucketName, prefix, delimiter, "", 2, false, ["Under1/bar"], ["Under1/baz/"], null);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterPercentage()
		{
			var bucketName = SetupObjects(["b%ar", "b%az", "c%ab", "foo"]);
			var client = GetClient();

			string delimiter = "%";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			Assert.Equal(["foo"], keys);

			var prefixes = response.CommonPrefixes;
			Assert.Equal(2, prefixes.Count);
			Assert.Equal(["b%", "c%"], prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 공백 구분자로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterWhitespace()
		{
			var bucketName = SetupObjects(["b ar", "b az", "c ab", "foo"]);
			var client = GetClient();

			string delimiter = " ";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			Assert.Equal(["foo"], keys);

			var prefixes = response.CommonPrefixes;
			Assert.Equal(2, prefixes.Count);
			Assert.Equal(["b ", "c "], prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 점[.] 구분자로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterDot()
		{
			var bucketName = SetupObjects(["b.ar", "b.az", "c.ab", "foo"]);
			var client = GetClient();

			string delimiter = ".";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			Assert.Equal(["foo"], keys);

			var prefixes = response.CommonPrefixes;
			Assert.Equal(2, prefixes.Count);
			Assert.Equal(["b.", "c."], prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 읽을수 없는 구분자[\\n]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterUnreadable()
		{
			var keyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			string delimiter = "\n";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;

			Assert.Equal(keyNames, keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterEmpty()
		{
			var keyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			string delimiter = "";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal("", response.Delimiter);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;

			Assert.Equal(keyNames, keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterNone()
		{
			var keyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var response = client.ListVersions(bucketName);
			Assert.Null(response.Delimiter);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;

			Assert.Equal(keyNames, keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "[폴더가 존재하지 않는 환경] 오브젝트 버전 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterNotExist()
		{
			var keyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			string delimiter = "/";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;

			Assert.Equal(keyNames, keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 특수문자가 생략되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsDelimiterNotSkipSpecial()
		{
			var keyNames = new List<string>();
			for (int i = 1000; i < 1999; i++) keyNames.Add("0/" + i.ToString());
			var keyNames2 = new List<string>() { "1999", "1999#", "1999+", "2000" };
			keyNames.AddRange(keyNames2);
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			string delimiter = "/";

			var response = client.ListVersions(bucketName, delimiter: delimiter);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;

			Assert.Equal(keyNames2, keys);
			Assert.Equal(["0/"], prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "prefix")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 접두사[/]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixBasic()
		{
			string prefix = "foo/";
			var bucketName = SetupObjects(["foo/bar", "foo/baz", "quux"]);
			var client = GetClient();

			var response = client.ListVersions(bucketName, prefix: prefix);
			Assert.Equal(prefix, response.Prefix);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Equal(["foo/bar", "foo/baz"], keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "prefix")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 접두사[ba]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixAlt()
		{
			string prefix = "ba";
			var bucketName = SetupObjects(["bar", "baz", "foo"]);
			var client = GetClient();

			var response = client.ListVersions(bucketName, prefix: prefix);
			Assert.Equal(prefix, response.Prefix);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Equal(["bar", "baz"], keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "prefix")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 접두사가 빈문자일때 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixEmpty()
		{
			var keyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			string prefix = "";
			var response = client.ListVersions(bucketName, prefix: prefix);
			Assert.Equal("", response.Prefix);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Equal(keyNames, keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "prefix")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 접두사를 입력하지 않아도 문제없는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixNone()
		{
			var keyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var response = client.ListVersions(bucketName);
			Assert.Equal("", response.Prefix);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Equal(keyNames, keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "prefix")]
		[Trait(MainData.Explanation, "[접두사와 일치하는 오브젝트가 존재하지 않는 환경] 오브젝트 버전 목록을 가져올때 접두사로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixNotExist()
		{
			var keyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			string prefix = "d";
			var response = client.ListVersions(bucketName, prefix: prefix);
			Assert.Equal(prefix, response.Prefix);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Empty(keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "prefix")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 읽을수 없는 접두사[\\n]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixUnreadable()
		{
			var keyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			string prefix = "\n";
			var response = client.ListVersions(bucketName, prefix: prefix);
			Assert.Equal(prefix, response.Prefix);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Empty(keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "PrefixAndDelimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 접두사와 구분자로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixDelimiterBasic()
		{
			var bucketName = SetupObjects(["foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"]);
			var client = GetClient();

			string prefix = "foo/";
			string delimiter = "/";
			var response = client.ListVersions(bucketName, delimiter: delimiter, prefix: prefix);

			Assert.Equal(prefix, response.Prefix);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Equal(["foo/bar"], keys);
			Assert.Equal(["foo/baz/"], prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "PrefixAndDelimiter")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 접두사[ba]와 구분자[a]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixDelimiterAlt()
		{
			var bucketName = SetupObjects(["bar", "bazar", "cab", "foo"]);
			var client = GetClient();

			string delimiter = "a";
			string prefix = "ba";

			var response = client.ListVersions(bucketName, delimiter: delimiter, prefix: prefix);

			Assert.Equal(prefix, response.Prefix);
			Assert.Equal(delimiter, response.Delimiter);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Equal(["bar"], keys);
			Assert.Equal(["baza"], prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "PrefixAndDelimiter")]
		[Trait(MainData.Explanation, "[접두사와 일치하는 오브젝트가 존재하지 않는 환경] 접두사와 구분자로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixDelimiterPrefixNotExist()
		{
			var bucketName = SetupObjects(["b/a/r", "b/a/c", "b/a/g", "g"]);
			var client = GetClient();

			var response = client.ListVersions(bucketName, delimiter: "d", prefix: "/");

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;

			Assert.Empty(keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "PrefixAndDelimiter")]
		[Trait(MainData.Explanation, "[구분자와 일치하는 오브젝트가 존재하지 않는 환경] 접두사와 구분자로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixDelimiterDelimiterNotExist()
		{
			var bucketName = SetupObjects(["b/a/c", "b/a/g", "b/a/r", "g"]);
			var client = GetClient();

			var response = client.ListVersions(bucketName, delimiter: "z", prefix: "b");

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;

			Assert.Equal(["b/a/c", "b/a/g", "b/a/r"], keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "PrefixAndDelimiter")]
		[Trait(MainData.Explanation, "[접두사와 구분자 모두 일치하는 오브젝트가 존재하지 않는 환경] 접두사와 구분자로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsPrefixDelimiterPrefixDelimiterNotExist()
		{
			var bucketName = SetupObjects(["b/a/r", "b/a/c", "b/a/g", "g"]);
			var client = GetClient();

			var response = client.ListVersions(bucketName, delimiter: "z", prefix: "y");

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;

			Assert.Empty(keys);
			Assert.Empty(prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "MaxKeys")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 최대목록갯수[1]로 제한되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMaxKeysOne()
		{
			var keyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var response = client.ListVersions(bucketName, maxKeys: 1);
			Assert.True(response.IsTruncated);

			var keys = GetKeys(response);
			Assert.Equal(keyNames.GetRange(0, 1), keys);

			response = client.ListVersions(bucketName, keyMarker: keyNames[0]);
			Assert.False(response.IsTruncated);

			keys = GetKeys(response);
			Assert.Equal(keyNames.GetRange(1, keyNames.Count - 1), keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "MaxKeys")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 최대목록갯수[0]로 제한되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMaxKeysZero()
		{
			var keyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var response = client.ListVersions(bucketName, maxKeys: 0);
			Assert.False(response.IsTruncated);
			var keys = GetKeys(response);
			Assert.Empty(keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "MaxKeys")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 최대목록갯수를 입력하지 않아도 문제없는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMaxKeysNone()
		{
			var keyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var response = client.ListVersions(bucketName, useLegacyMaxKeys: false);
			Assert.False(response.IsTruncated);
			var keys = GetKeys(response);
			Assert.Equal(keyNames, keys);
			Assert.Equal(1000, response.MaxKeys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "오브젝트 버전 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMarkerNone()
		{
			var keyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var response = client.ListVersions(bucketName, keyMarker: "");
			Assert.Null(response.NextKeyMarker);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "빈 마커를 입력하고 오브젝트 버전 목록을 불러올때 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMarkerEmpty()
		{
			var keyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var response = client.ListVersions(bucketName, keyMarker: "");
			Assert.Null(response.NextKeyMarker);
			Assert.False(response.IsTruncated);
			var keys = GetKeys(response);
			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "마커에 읽을수 없는 값[\\n]을 설정한 경우 오브젝트 버전 목록을 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMarkerUnreadable()
		{
			var keyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var marker = "\n";

			var response = client.ListVersions(bucketName, keyMarker: marker);
			Assert.Null(response.NextKeyMarker);
			Assert.False(response.IsTruncated);
			var keys = GetKeys(response);
			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "[마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] " +
			"마커를 설정하고 오브젝트 버전 목록을 불러올때 재대로 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMarkerNotInList()
		{
			var keyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var marker = "blah";

			var response = client.ListVersions(bucketName, keyMarker: marker);
			Assert.Equal(marker, response.KeyMarker);
			var keys = GetKeys(response);
			Assert.Equal(["foo", "quxx"], keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "[마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경]" +
									 "마커를 설정하고 오브젝트 버전 목록을 불러올때 재대로 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsMarkerAfterList()
		{
			var keyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var marker = "zzz";

			var response = client.ListVersions(bucketName, keyMarker: marker);
			Assert.Equal(marker, response.KeyMarker);
			Assert.False(response.IsTruncated);
			var keys = GetKeys(response);
			Assert.Empty(keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "Version정보를 가질 수 있는 버킷에서 ListObjectsVersions로 가져온 Metadata와 " +
									 "HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsReturnData()
		{
			var bucketName = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var keyNames = new List<string>() { "bar", "baz", "foo" };
			bucketName = SetupObjects(keyNames, bucketName: bucketName);

			var client = GetClient();
			var data = new List<ObjectData>();

			foreach (var key in keyNames)
			{
				var objResponse = client.GetObjectMetadata(bucketName, key);
				var aclResponse = client.GetObjectACL(bucketName, key);

				data.Add(new ObjectData()
				{
					Key = key,
					DisplayName = aclResponse.AccessControlList.Owner.DisplayName,
					Id = aclResponse.AccessControlList.Owner.Id,
					ETag = objResponse.ETag,
					LastModified = objResponse.LastModified.Value,
					ContentLength = objResponse.ContentLength,
					VersionId = objResponse.VersionId
				});
			}

			var response = client.ListVersions(bucketName);
			var objList = response.Versions;

			foreach (var obj in objList)
			{
				var keyName = obj.Key;
				var keyData = GetObjectToKey(keyName, data);

				Assert.NotNull(keyData);
				Assert.Equal(keyData.ETag, obj.ETag);
				Assert.Equal(keyData.ContentLength, obj.Size);
				Assert.Equal(keyData.DisplayName, obj.Owner.DisplayName);
				Assert.Equal(keyData.Id, obj.Owner.Id);
				Assert.Equal(keyData.VersionId, obj.VersionId);
				Assert.Equal(keyData.LastModified, obj.LastModified.Value);
			}
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 버전 목록을 읽을수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListVersionsObjectsAnonymous()
		{
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead);

			var unauthenticatedClient = GetUnauthenticatedClient();
			unauthenticatedClient.ListVersions(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "권한없는 사용자가 버킷의 오브젝트 버전 목록을 읽지 못하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketListVersionsObjectsAnonymousFail()
		{
			var bucketName = GetNewBucket();
			var unauthenticatedClient = GetUnauthenticatedClient();

			var e = Assert.Throws<AggregateException>(() => unauthenticatedClient.ListVersions(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷 내 오브젝트 버전들을 가져오려 했을 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketListVersionsNotExist()
		{
			var bucketName = GetNewBucketName(false);
			var client = GetClient();

			var e = Assert.Throws<AggregateException>(() => client.ListVersions(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Filtering")]
		[Trait(MainData.Explanation, "버전 목록 조회 시 delimiter, maxKeys, keyMarker 필터링이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningBucketListFilteringAll()
		{
			var keyNames = new List<string>() { "test1/f1", "test2/f2", "test3", "test4/f3", "testF4" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var marker = "test3";
			var delimiter = "/";
			var maxKeys = 3;

			var response = client.ListVersions(bucketName, delimiter: delimiter, maxKeys: maxKeys);
			Assert.Equal(delimiter, response.Delimiter);
			Assert.Equal(maxKeys, response.MaxKeys);
			Assert.Equal(marker, response.NextKeyMarker);
			Assert.True(response.IsTruncated);

			var keys = GetKeys(response);
			var prefixes = response.CommonPrefixes;
			Assert.Equal(["test3"], keys);
			Assert.Equal(["test1/", "test2/"], prefixes);

			response = client.ListVersions(bucketName, delimiter: delimiter, maxKeys: maxKeys, keyMarker: marker);
			Assert.Equal(delimiter, response.Delimiter);
			Assert.Equal(maxKeys, response.MaxKeys);
			Assert.False(response.IsTruncated);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버전이 활성화된 버킷에서 동일 키의 여러 버전이 올바른 순서로 목록에 포함되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningObjListMarker()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var keyName = "testVersioningObjListMarker";
			var objects = new List<string>();

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			for (var i = 0; i < 10; i++)
			{
				var response = client.PutObject(bucketName, keyName, keyName + i);
				objects.Insert(0, response.VersionId);
			}

			var listResponse = client.ListVersions(bucketName);
			Assert.Equal(objects.Count, listResponse.Versions.Count);

			for (var i = 0; i < objects.Count; i++)
				Assert.Equal(objects[i], listResponse.Versions[i].VersionId);
		}
	}
}
