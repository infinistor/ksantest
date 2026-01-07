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
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class ListObjectsV2 : TestBase
	{
		public ListObjectsV2(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 오브젝트 목록을 올바르게 가져오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListV2Many()
		{
			var bucketName = SetupObjects(["foo", "bar", "baz"]);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName, maxKeys: 2);
			Assert.Equal(["bar", "baz"], GetKeys(Response));
			Assert.Equal(2, Response.S3Objects.Count);
			Assert.True(Response.IsTruncated);

			Response = client.ListObjectsV2(bucketName, startAfter: "baz", maxKeys: 2);
			Assert.Equal(["foo"], GetKeys(Response));
			Assert.Single(Response.S3Objects);
			Assert.False(Response.IsTruncated);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Key count")]
		[Trait(MainData.Explanation, "ListObjectV2로 오브젝트 목록을 가져올때 Key Count 값을 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBasicKeyCount()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			for (int i = 0; i < 5; i++) client.PutObject(bucketName, i.ToString());

			var Response = client.ListObjectsV2(bucketName);

			Assert.Equal(5, Response.KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_basic()
		{
			var bucketName = SetupObjects(["foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"]);
			var client = GetClient();

			string MyDelimiter = "/";

			var Response = client.ListObjectsV2(bucketName, delimiter: MyDelimiter);
			Assert.Equal(MyDelimiter, Response.Delimiter);
			Assert.Equal(["asdf"], GetKeys(Response));

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(2, Prefixes.Count);
			Assert.Equal(["foo/", "quux/"], Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Encoding")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		[Trait(MainData.Different, MainData.True)]
		public void test_bucket_listv2_encoding_basic()
		{
			var bucketName = SetupObjects(["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"]);
			var client = GetClient();

			string Delimiter = "/";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter, encodingTypeName: "url");
			Assert.Equal(Delimiter, Response.Delimiter);
			Assert.Equal(["asdf%2Bb"], GetKeys(Response));

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(3, Prefixes.Count);
			Assert.Equal(["foo%2B1/", "foo/", "quux+ab/"], Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter and Prefix")]
		[Trait(MainData.Explanation, "조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjcetsv2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_prefix()
		{
			var bucketName = SetupObjects(["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"]);

			string Delimiter = "/";
			string ContinuationToken = "";
			string prefix = "";

			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, Delimiter, null, 1, true, ["asdf"], []);
			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, Delimiter, ContinuationToken, 1, true, EmptyList, ["boo/"]);
			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, Delimiter, ContinuationToken, 1, false, EmptyList, ["cquux/"], true);

			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, Delimiter, null, 2, true, ["asdf"], ["boo/"]);
			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, Delimiter, ContinuationToken, 2, false, EmptyList, ["cquux/"], true);

			prefix = "boo/";

			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, Delimiter, null, 1, true, ["boo/bar"], []);
			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, Delimiter, ContinuationToken, 1, false, EmptyList, ["boo/baz/"], true);

			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, Delimiter, null, 2, false, ["boo/bar"], ["boo/baz/"], true);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter and Prefix")]
		[Trait(MainData.Explanation, "비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_prefix_ends_with_delimiter()
		{
			var bucketName = SetupObjects(["asdf/"], body: "");
			ValidateListObjcetV2(bucketName, "asdf/", "/", null, 1000, false, ["asdf/"], EmptyList, true);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_alt()
		{
			var bucketName = SetupObjects(["bar", "baz", "cab", "foo"]);
			var client = GetClient();

			string Delimiter = "a";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			Assert.Equal(["foo"], Keys);

			var Profixes = Response.CommonPrefixes;
			Assert.Equal(2, Profixes.Count);
			Assert.Equal(["ba", "ca"], Profixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter and Prefix")]
		[Trait(MainData.Explanation, "[폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_prefix_underscore()
		{
			var bucketName = SetupObjects(["_obj1_", "_under1/bar", "_under1/baz/xyzzy", "_under2/thud", "_under2/bla"]);

			string delim = "/";
			string ContinuationToken = "";
			string prefix = "";

			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, delim, null, 1, true, ["_obj1_"], []);
			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, delim, ContinuationToken, 1, true, EmptyList, ["_under1/"]);
			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, delim, ContinuationToken, 1, false, EmptyList, ["_under2/"], true);

			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, delim, null, 2, true, ["_obj1_"], ["_under1/"]);
			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, delim, ContinuationToken, 2, false, EmptyList, ["_under2/"], true);

			prefix = "_under1/";

			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, delim, null, 1, true, ["_under1/bar"], []);
			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, delim, ContinuationToken, 1, false, EmptyList, ["_under1/baz/"], true);

			ContinuationToken = ValidateListObjcetV2(bucketName, prefix, delim, null, 2, false, ["_under1/bar"], ["_under1/baz/"], true);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_percentage()
		{
			var bucketName = SetupObjects(["b%ar", "b%az", "c%ab", "foo"]);
			var client = GetClient();

			string Delimiter = "%";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			Assert.Equal(["foo"], Keys);

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(2, Prefixes.Count);
			Assert.Equal(["b%", "c%"], Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		[Trait(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 공백일경우 string.Empty 반환
		public void test_bucket_listv2_delimiter_whitespace()
		{
			var bucketName = SetupObjects(["b ar", "b az", "c ab", "foo"]);
			var client = GetClient();

			string Delimiter = " ";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter);
			Assert.Empty(Response.Delimiter);

			var Keys = GetKeys(Response);
			Assert.Equal(["foo"], Keys);

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(2, Prefixes.Count);
			Assert.Equal(["b ", "c "], Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_dot()
		{
			var bucketName = SetupObjects(["b.ar", "b.az", "c.ab", "foo"]);
			var client = GetClient();

			string Delimiter = ".";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			Assert.Equal(["foo"], Keys);

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(2, Prefixes.Count);
			Assert.Equal(["b.", "c."], Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 읽을수 없는 구분자[\\n]로 필터링 되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		[Trait(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 \x0a일경우 string.Empty 반환
		public void test_bucket_listv2_delimiter_unreadable()
		{
			var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "\x0a";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter);
			Assert.Empty(Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_empty()
		{
			var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter);
			Assert.Null(Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_none()
		{
			var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName);
			Assert.Null(Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Fetchowner")]
		[Trait(MainData.Explanation, "[권한정보를 가져오도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_fetchowner_notempty()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName, fetchOwner: true);
			var ObjectList = Response.S3Objects;

			Assert.NotNull(ObjectList[0].Owner);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Fetchowner")]
		[Trait(MainData.Explanation, "[default = 권한정보를 가져오지 않음] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_fetchowner_defaultempty()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName);
			var ObjectList = Response.S3Objects;

			Assert.Null(ObjectList[0].Owner);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Fetchowner")]
		[Trait(MainData.Explanation, "[권한정보를 가져오지 않도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_fetchowner_empty()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName, fetchOwner: false);
			var ObjectList = Response.S3Objects;

			Assert.Null(ObjectList[0].Owner);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "[폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_delimiter_not_exist()
		{
			var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "/";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "[접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_basic()
		{
			var bucketName = SetupObjects(["foo/bar", "foo/baz", "quux"]);
			var client = GetClient();

			string Prefix = "foo/";
			var Response = client.ListObjectsV2(bucketName, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(["foo/bar", "foo/baz"], Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_alt()
		{
			var bucketName = SetupObjects(["bar", "baz", "foo"]);
			var client = GetClient();

			string Prefix = "ba";
			var Response = client.ListObjectsV2(bucketName, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(["bar", "baz"], Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_empty()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Prefix = "";
			var Response = client.ListObjectsV2(bucketName, prefix: Prefix);
			Assert.Empty(Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_none()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName);
			Assert.True(string.IsNullOrWhiteSpace(Response.Prefix));

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "[접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_not_exist()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Prefix = "d";
			var Response = client.ListObjectsV2(bucketName, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Empty(Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		[Trait(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 \x0a일경우 string.Empty 반환
		public void test_bucket_listv2_prefix_unreadable()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Prefix = "\x0a";
			var Response = client.ListObjectsV2(bucketName, prefix: Prefix);
			Assert.Empty(Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Empty(Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_delimiter_basic()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Prefix = "foo/";
			string Delimiter = "/";
			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(["foo/bar"], Keys);
			Assert.Equal(["foo/baz/"], Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "[구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_delimiter_alt()
		{
			var KeyNames = new List<string>() { "bar", "bazar", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "a";
			string Prefix = "ba";

			var Response = client.ListObjectsV2(bucketName, delimiter: Delimiter, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(["bar"], Keys);
			Assert.Equal(["baza"], Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "[입력한 접두어와 일치하는 오브젝트가 없을 경우]" +
									 " 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_delimiter_prefix_not_exist()
		{
			var bucketName = SetupObjects(["b/a/r", "b/a/c", "b/a/g", "g"]);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName, delimiter: "d", prefix: "/");

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Empty(Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "[구분자가 '/'가 아닐 경우]" +
									 "접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_delimiter_delimiter_not_exist()
		{
			var bucketName = SetupObjects(["b/a/c", "b/a/g", "b/a/r", "g"]);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName, delimiter: "z", prefix: "b");

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(["b/a/c", "b/a/g", "b/a/r"], Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "[구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우]" +
									 "접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist()
		{
			var bucketName = SetupObjects(["b/a/r", "b/a/c", "b/a/g", "g"]);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName, delimiter: "z", prefix: "y");

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Empty(Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "maxKeys")]
		[Trait(MainData.Explanation, "오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_maxkeys_one()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName, maxKeys: 1);
			Assert.True(Response.IsTruncated);

			var Keys = GetKeys(Response);
			Assert.Equal(KeyNames.GetRange(0, 1), Keys);

			Response = client.ListObjectsV2(bucketName, startAfter: KeyNames[0]);
			Assert.False(Response.IsTruncated);

			Keys = GetKeys(Response);
			Assert.Equal(KeyNames.GetRange(1, KeyNames.Count - 1), Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "maxKeys")]
		[Trait(MainData.Explanation, "오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_maxkeys_zero()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName, maxKeys: 0);

			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Empty(Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "maxKeys")]
		[Trait(MainData.Explanation, "[default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_maxkeys_none()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjectsV2(bucketName);
			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Equal(KeyNames, Keys);
			Assert.Equal(1000, Response.MaxKeys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Continuationtoken")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 다음 토큰값을 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_continuationtoken()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response1 = client.ListObjectsV2(bucketName, maxKeys: 1);
			var NextContinuationToken = Response1.NextContinuationToken;

			var Response2 = client.ListObjectsV2(bucketName, continuationToken: NextContinuationToken);
			Assert.Equal(NextContinuationToken, Response2.ContinuationToken);
			Assert.False(Response2.IsTruncated);
			var KeyNames2 = new List<string>() { "baz", "foo", "quxx" };
			var Keys = GetKeys(Response2);
			Assert.Equal(KeyNames2, Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "Continuationtoken and startAfter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 Startafter와 토큰이 재대로 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_both_continuationtoken_startafter()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var startAfter = "bar";

			var Response1 = client.ListObjectsV2(bucketName, startAfter: startAfter, maxKeys: 1);
			var NextContinuationToken = Response1.NextContinuationToken;


			var Response2 = client.ListObjectsV2(bucketName, startAfter: startAfter, continuationToken: NextContinuationToken);
			Assert.Equal(NextContinuationToken, Response2.ContinuationToken);
			//Assert.Equal(startAfter, Response2.StartAfter);
			Assert.False(Response2.IsTruncated);
			var KeyNames2 = new List<string>() { "foo", "quxx" };
			var Keys = GetKeys(Response2);
			Assert.Equal(KeyNames2, Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "startAfter")]
		[Trait(MainData.Explanation, "StartAfter에 읽을수 없는 값[\\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_startafter_unreadable()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var startAfter = "\x0a";

			var Response = client.ListObjectsV2(bucketName, startAfter: startAfter);
			Assert.Empty(Response.StartAfter);
			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Equal(KeyNames, Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "startAfter")]
		[Trait(MainData.Explanation, "[StartAfter와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] " +
			"마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_startafter_not_in_list()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var startAfter = "blah";

			var Response = client.ListObjectsV2(bucketName, startAfter: startAfter);
			Assert.Equal(startAfter, Response.StartAfter);
			var Keys = GetKeys(Response);
			Assert.Equal(["foo", "quxx"], Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "startAfter")]
		[Trait(MainData.Explanation, "[StartAfter와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경]" +
									 "StartAfter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_startafter_after_list()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var startAfter = "zzz";

			var Response = client.ListObjectsV2(bucketName, startAfter: startAfter);
			Assert.Equal(startAfter, Response.StartAfter);
			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Empty(Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_listv2_objects_anonymous()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketACL(bucketName, acl: Amazon.S3.S3CannedACL.PublicRead);

			var UnauthenticatedClient = GetUnauthenticatedClient();
			UnauthenticatedClient.ListObjectsV2(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_listv2_objects_anonymous_fail()
		{
			var bucketName = GetNewBucket();

			var UnauthenticatedClient = GetUnauthenticatedClient();

			var e = Assert.Throws<AggregateException>(() => UnauthenticatedClient.ListObjectsV2(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ListObjectsV2")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucketv2_notexist()
		{
			var bucketName = GetNewBucketName(false);
			var client = GetClient();

			var e = Assert.Throws<AggregateException>(() => client.ListObjectsV2(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}
	}
}
