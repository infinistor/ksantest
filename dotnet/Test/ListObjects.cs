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
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
{
	public class ListObjects : TestBase
	{
		public ListObjects(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 오브젝트 목록을 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListMany()
		{
			var bucketName = SetupObjects(new List<string>() { "foo", "bar", "baz" });
			var client = GetClient();

			var Response = client.ListObjects(bucketName, maxKeys: 2);
			Assert.Equal(new List<string>() { "bar", "baz" }, GetKeys(Response));
			Assert.Equal(2, Response.S3Objects.Count);
			Assert.True(Response.IsTruncated);

			Response = client.ListObjects(bucketName, marker: "baz", maxKeys: 2);
			Assert.Equal(new List<string>() { "foo" }, GetKeys(Response));
			Assert.Single(Response.S3Objects);
			Assert.False(Response.IsTruncated);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListDelimiterBasic()
		{
			var bucketName = SetupObjects(new List<string>() { "foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf" });
			var client = GetClient();

			string MyDelimiter = "/";

			var Response = client.ListObjects(bucketName, delimiter: MyDelimiter);
			Assert.Equal(MyDelimiter, Response.Delimiter);
			Assert.Equal(new List<string>() { "asdf" }, GetKeys(Response));

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(2, Prefixes.Count);
			Assert.Equal(new List<string>() { "foo/", "quux/" }, Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Encoding")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		[Trait(MainData.Different, MainData.True)]
		public void test_bucket_list_encoding_basic()
		{
			var bucketName = SetupObjects(new List<string>() { "foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b" });
			var client = GetClient();

			string Delimiter = "/";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter, encodingTypeName: "url");
			Assert.Equal(Delimiter, Response.Delimiter);
			Assert.Equal(new List<string>() { "asdf%2Bb" }, GetKeys(Response));

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(3, Prefixes.Count);
			Assert.Equal(new List<string>() { "foo%2B1/", "foo/", "quux+ab/" }, Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter and Prefix")]
		[Trait(MainData.Explanation, "조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_prefix()
		{
			var bucketName = SetupObjects(new List<string>() { "asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla" });

			string Delimiter = "/";
			string marker = string.Empty;
			string Prefix = string.Empty;

			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, "", 1, true, new List<string>() { "asdf" }, EmptyList, "asdf");
			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, marker, 1, true, EmptyList, new List<string>() { "boo/" }, "boo/");
			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, marker, 1, false, EmptyList, new List<string>() { "cquux/" }, null);

			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, "", 2, true, new List<string>() { "asdf" }, new List<string>() { "boo/" }, "boo/");
			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, marker, 2, false, EmptyList, new List<string>() { "cquux/" }, null);

			Prefix = "boo/";

			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, "", 1, true, new List<string>() { "boo/bar" }, EmptyList, "boo/bar");
			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, marker, 1, false, EmptyList, new List<string>() { "boo/baz/" }, null);

			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, "", 2, false, new List<string>() { "boo/bar" }, new List<string>() { "boo/baz/" }, null);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter and Prefix")]
		[Trait(MainData.Explanation, "비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_prefix_ends_with_delimiter()
		{
			var bucketName = SetupObjects(new List<string>() { "asdf/" }, body: "");
			ValidateListObjcet(bucketName, "asdf/", "/", "", 1000, false, new List<string>() { "asdf/" }, EmptyList, null);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_alt()
		{
			var bucketName = SetupObjects(new List<string>() { "bar", "baz", "cab", "foo" });
			var client = GetClient();

			string Delimiter = "a";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			Assert.Equal(new List<string>() { "foo" }, Keys);

			var Profixes = Response.CommonPrefixes;
			Assert.Equal(2, Profixes.Count);
			Assert.Equal(new List<string>() { "ba", "ca" }, Profixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter and Prefix")]
		[Trait(MainData.Explanation, "[폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_prefix_underscore()
		{
			var bucketName = SetupObjects(new List<string>() { "_obj1_", "_under1/bar", "_under1/baz/xyzzy", "_under2/thud", "_under2/bla" });

			string Delimiter = "/";
			string marker = "";
			string Prefix = "";

			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, "", 1, true, new List<string>() { "_obj1_" }, EmptyList, "_obj1_");
			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, marker, 1, true, EmptyList, new List<string>() { "_under1/" }, "_under1/");
			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, marker, 1, false, EmptyList, new List<string>() { "_under2/" }, null);

			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, "", 2, true, new List<string>() { "_obj1_" }, new List<string>() { "_under1/" }, "_under1/");
			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, marker, 2, false, EmptyList, new List<string>() { "_under2/" }, null);

			Prefix = "_under1/";

			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, "", 1, true, new List<string>() { "_under1/bar" }, EmptyList, "_under1/bar");
			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, marker, 1, false, EmptyList, new List<string>() { "_under1/baz/" }, null);

			marker = ValidateListObjcet(bucketName, Prefix, Delimiter, "", 2, false, new List<string>() { "_under1/bar" }, new List<string>() { "_under1/baz/" }, null);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_percentage()
		{
			var bucketName = SetupObjects(new List<string>() { "b%ar", "b%az", "c%ab", "foo" });
			var client = GetClient();

			string Delimiter = "%";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			Assert.Equal(new List<string>() { "foo" }, Keys);

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(2, Prefixes.Count);
			Assert.Equal(new List<string>() { "b%", "c%" }, Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		[Trait(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 공백일경우 string.Empty 반환
		public void test_bucket_list_delimiter_whitespace()
		{
			var bucketName = SetupObjects(new List<string>() { "b ar", "b az", "c ab", "foo" });
			var client = GetClient();

			string Delimiter = " ";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter);
			Assert.Empty(Response.Delimiter);

			var Keys = GetKeys(Response);
			Assert.Equal(new List<string>() { "foo" }, Keys);

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(2, Prefixes.Count);
			Assert.Equal(new List<string>() { "b ", "c " }, Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_dot()
		{
			var bucketName = SetupObjects(new List<string>() { "b.ar", "b.az", "c.ab", "foo" });
			var client = GetClient();

			string Delimiter = ".";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			Assert.Equal(new List<string>() { "foo" }, Keys);

			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(2, Prefixes.Count);
			Assert.Equal(new List<string>() { "b.", "c." }, Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 읽을수 없는 구분자[\\n]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		[Trait(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 \x0a일경우 string.Empty 반환
		public void test_bucket_list_delimiter_unreadable()
		{
			var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "\x0a";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter);
			Assert.Empty(Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_empty()
		{
			var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter);
			Assert.Null(Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_none()
		{
			var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjects(bucketName);
			Assert.Null(Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "[폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_delimiter_not_exist()
		{
			var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "/";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Delimiter")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 특수문자가 생략되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)] // Default maxKeys = 1000이라 목록을 다 읽어 올 수 없어 늘렸음
		public void test_bucket_list_delimiter_not_skip_special()
		{
			var KeyNames = new List<string>();
			for (int i = 1000; i < 1999; i++) KeyNames.Add("0/" + i.ToString());
			var KeyNames2 = new List<string>() { "1999", "1999#", "1999+", "2000" };
			KeyNames.AddRange(KeyNames2);
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "/";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter, maxKeys: 2000);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;

			Assert.Equal(KeyNames2, Keys);
			Assert.Equal(new List<string>() { "0/" }, Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "[접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_basicst()
		{
			var bucketName = SetupObjects(new List<string>() { "foo/bar", "foo/baz", "quux" });
			var client = GetClient();

			string Prefix = "foo/";
			var Response = client.ListObjects(bucketName, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(new List<string>() { "foo/bar", "foo/baz" }, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_alt()
		{
			var bucketName = SetupObjects(new List<string>() { "bar", "baz", "foo" });
			var client = GetClient();

			string Prefix = "ba";
			var Response = client.ListObjects(bucketName, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(new List<string>() { "bar", "baz" }, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_empty()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Prefix = "";
			var Response = client.ListObjects(bucketName, prefix: Prefix);
			Assert.Empty(Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_none()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjects(bucketName);
			Assert.True(string.IsNullOrWhiteSpace(Response.Prefix));


			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(KeyNames, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "[접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_not_exist()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Prefix = "d";
			var Response = client.ListObjects(bucketName, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Empty(Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		[Trait(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 \x0a일경우 string.Empty 반환
		public void test_bucket_list_prefix_unreadable()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Prefix = "\x0a";
			var Response = client.ListObjects(bucketName, prefix: Prefix);
			Assert.Empty(Response.Prefix);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Empty(Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_delimiter_basic()
		{
			var KeyNames = new List<string>() { "foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Prefix = "foo/";
			string Delimiter = "/";
			var Response = client.ListObjects(bucketName, delimiter: Delimiter, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(new List<string>() { "foo/bar" }, Keys);
			Assert.Equal(new List<string>() { "foo/baz/" }, Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "[구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_delimiter_alt()
		{
			var KeyNames = new List<string>() { "bar", "bazar", "cab", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			string Delimiter = "a";
			string Prefix = "ba";

			var Response = client.ListObjects(bucketName, delimiter: Delimiter, prefix: Prefix);
			Assert.Equal(Prefix, Response.Prefix);
			Assert.Equal(Delimiter, Response.Delimiter);

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(new List<string>() { "bar" }, Keys);
			Assert.Equal(new List<string>() { "baza" }, Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "[입력한 접두어와 일치하는 오브젝트가 없을 경우]" +
									 "접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_delimiter_prefix_not_exist()
		{
			var bucketName = SetupObjects(new List<string>() { "b/a/r", "b/a/c", "b/a/g", "g" });
			var client = GetClient();

			var Response = client.ListObjects(bucketName, delimiter: "d", prefix: "/");

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Empty(Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "[구분자가 '/'가 아닐 경우]" +
									 "접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_delimiter_delimiter_not_exist()
		{
			var bucketName = SetupObjects(new List<string>() { "b/a/c", "b/a/g", "b/a/r", "g" });
			var client = GetClient();

			var Response = client.ListObjects(bucketName, delimiter: "z", prefix: "b");

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Equal(new List<string>() { "b/a/c", "b/a/g", "b/a/r" }, Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Prefix and Delimiter")]
		[Trait(MainData.Explanation, "[구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우]" +
									 "접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist()
		{
			var bucketName = SetupObjects(new List<string>() { "b/a/r", "b/a/c", "b/a/g", "g" });
			var client = GetClient();

			var Response = client.ListObjects(bucketName, delimiter: "z", prefix: "y");

			var Keys = GetKeys(Response);
			var Prefixes = Response.CommonPrefixes;
			Assert.Empty(Keys);
			Assert.Empty(Prefixes);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "maxKeys")]
		[Trait(MainData.Explanation, "오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_maxkeys_one()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjects(bucketName, maxKeys: 1);
			Assert.True(Response.IsTruncated);

			var Keys = GetKeys(Response);
			Assert.Equal(KeyNames.GetRange(0, 1), Keys);

			Response = client.ListObjects(bucketName, marker: KeyNames[0]);
			Assert.False(Response.IsTruncated);

			Keys = GetKeys(Response);
			Assert.Equal(KeyNames.GetRange(1, KeyNames.Count - 1), Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "maxKeys")]
		[Trait(MainData.Explanation, "오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_maxkeys_zero()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjects(bucketName, maxKeys: 0);

			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Empty(Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "maxKeys")]
		[Trait(MainData.Explanation, "[default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_maxkeys_none()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjects(bucketName);
			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Equal(KeyNames, Keys);
			Assert.Equal(1000, Response.MaxKeys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "maxKeys")]
		[Trait(MainData.Explanation, "[함수가 호출되기 전에 URL에 유효하지 않은 최대목록갯수를 추가할 경우] 오브젝트 목록 조회 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_list_maxkeys_invalid()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			client.Client.BeforeRequestEvent += BeforeCallS3ListObjectsMaxKeys;

			var e = Assert.Throws<AggregateException>(() => client.ListObjects(bucketName));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		private void BeforeCallS3ListObjectsMaxKeys(object sender, RequestEventArgs e)
		{
			var requestEvent = e as WebServiceRequestEventArgs;
			requestEvent.ParameterCollection.Add("max-keys", "blah");
			//requestEvent.Headers.Add("max-keys", "blah");
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_marker_none()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjects(bucketName, marker: "");
			Assert.Null(Response.NextMarker);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_marker_empty()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Response = client.ListObjects(bucketName, marker: "");
			Assert.Null(Response.NextMarker);
			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Equal(KeyNames, Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "마커에 읽을수 없는 값[\\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_marker_unreadable()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var marker = "\x0a";

			var Response = client.ListObjects(bucketName, marker: marker);
			Assert.Null(Response.NextMarker);
			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Equal(KeyNames, Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "[마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] " +
			"마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_marker_not_in_list()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var marker = "blah";

			var Response = client.ListObjects(bucketName, marker: marker);
			var Keys = GetKeys(Response);
			Assert.Equal(new List<string>() { "foo", "quxx" }, Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "marker")]
		[Trait(MainData.Explanation, "[마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경]" +
									 "마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_marker_after_list()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var marker = "zzz";

			var Response = client.ListObjects(bucketName, marker: marker);
			Assert.False(Response.IsTruncated);
			var Keys = GetKeys(Response);
			Assert.Empty(Keys);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "ListObjcets으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_return_data()
		{
			var KeyNames = new List<string>() { "bar", "baz", "foo" };
			var bucketName = SetupObjects(KeyNames);
			var client = GetClient();

			var Data = new List<ObjectData>();


			foreach (var Key in KeyNames)
			{
				var ObjResponse = client.GetObjectMetadata(bucketName, Key);
				var ACLResponse = client.GetObjectACL(bucketName, Key);

				Data.Add(new ObjectData()
				{
					Key = Key,
					DisplayName = ACLResponse.AccessControlList.Owner.DisplayName,
					Id = ACLResponse.AccessControlList.Owner.Id,
					ETag = ObjResponse.ETag,
					LastModified = ObjResponse.LastModified,
					ContentLength = ObjResponse.ContentLength,
				});
			}

			var Response = client.ListObjects(bucketName);
			var ObjList = Response.S3Objects;

			foreach (var Object in ObjList)
			{
				var KeyName = Object.Key;
				var KeyData = GetObjectToKey(KeyName, Data);

				Assert.NotNull(KeyData);
				Assert.Equal(KeyData.ETag, Object.ETag);
				Assert.Equal(KeyData.ContentLength, Object.Size);
				Assert.Equal(KeyData.DisplayName, Object.Owner.DisplayName);
				Assert.Equal(KeyData.Id, Object.Owner.Id);
				Assert.Equal(KeyData.LastModified, Object.LastModified.ToUniversalTime());
			}
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_objects_anonymous()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketACL(bucketName, acl: Amazon.S3.S3CannedACL.PublicRead);

			var UnauthenticatedClient = GetUnauthenticatedClient();
			UnauthenticatedClient.ListObjects(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_list_objects_anonymous_fail()
		{
			var bucketName = GetNewBucket();
			var unauthenticatedClient = GetUnauthenticatedClient();

			var e = Assert.Throws<AggregateException>(() => unauthenticatedClient.ListObjects(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ListObjects")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_notexist()
		{
			var bucketName = GetNewBucketName(false);
			var client = GetClient();

			var e = Assert.Throws<AggregateException>(() => client.ListObjects(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}
	}
}
