/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version 
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
using Amazon.Runtime;
using System.Collections.Generic;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Amazon.S3;

namespace s3tests2
{
    [TestClass]
    public class ListObjects : TestBase
    {
        [TestMethod("test_bucket_list_many")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "버킷의 오브젝트 목록을 올바르게 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_many()
        {
            var BucketName = SetupObjects(new List<string>() { "foo", "bar", "baz" });
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName, MaxKeys: 2);
            CollectionAssert.AreEqual(new List<string>() { "bar", "baz" }, GetKeys(Response));
            Assert.AreEqual(2, Response.S3Objects.Count);
            Assert.IsTrue(Response.IsTruncated);

            Response = Client.ListObjects(BucketName, Marker: "baz", MaxKeys: 2);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, GetKeys(Response));
            Assert.AreEqual(1, Response.S3Objects.Count);
            Assert.IsFalse(Response.IsTruncated);
        }

        [TestMethod("test_bucket_list_delimiter_basic")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_basic()
        {
            var BucketName = SetupObjects(new List<string>() { "foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf" });
            var Client = GetClient();

            string MyDelimiter = "/";

            var Response = Client.ListObjects(BucketName, Delimiter: MyDelimiter);
            Assert.AreEqual(MyDelimiter, Response.Delimiter);
            CollectionAssert.AreEqual(new List<string>() { "asdf" }, GetKeys(Response));

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "foo/", "quux/" }, Prefixes);
        }

        [TestMethod("test_bucket_list_encoding_basic")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Encoding")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        [TestProperty(MainData.Different, MainData.True)]
        public void test_bucket_list_encoding_basic()
        {
            var BucketName = SetupObjects(new List<string>() { "foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b" });
            var Client = GetClient();

            string Delimiter = "/";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter, EncodingTypeName: "url");
            Assert.AreEqual(Delimiter, Response.Delimiter);
            CollectionAssert.AreEqual(new List<string>() { "asdf%2Bb" }, GetKeys(Response));

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(3, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "foo%2B1/", "foo/", "quux+ab/" }, Prefixes);
        }

        [TestMethod("test_bucket_list_delimiter_prefix")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter and Prefix")]
        [TestProperty(MainData.Explanation, "조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_prefix()
        {
            var BucketName = SetupObjects(new List<string>() { "asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla" });

            string Delimiter = "/";
            string Marker = string.Empty;
            string Prefix = string.Empty;

            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 1, true, new List<string>() { "asdf" }, EmptyList, "asdf");
            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, true, EmptyList, new List<string>() { "boo/" }, "boo/");
            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, false, EmptyList, new List<string>() { "cquux/" }, null);

            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 2, true, new List<string>() { "asdf" }, new List<string>() { "boo/" }, "boo/");
            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 2, false, EmptyList, new List<string>() { "cquux/" }, null);

            Prefix = "boo/";

            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 1, true, new List<string>() { "boo/bar" }, EmptyList, "boo/bar");
            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, false, EmptyList, new List<string>() { "boo/baz/" }, null);

            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 2, false, new List<string>() { "boo/bar" }, new List<string>() { "boo/baz/" }, null);
        }

        [TestMethod("test_bucket_list_delimiter_prefix_ends_with_delimiter")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter and Prefix")]
        [TestProperty(MainData.Explanation, "비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_prefix_ends_with_delimiter()
        {
            var BucketName = SetupObjects(new List<string>() { "asdf/" }, Body:"");
            ValidateListObjcet(BucketName, "asdf/", "/", "", 1000, false, new List<string>() { "asdf/" }, EmptyList, null);
        }

        [TestMethod("test_bucket_list_delimiter_alt")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_alt()
        {
            var BucketName = SetupObjects(new List<string>() { "bar", "baz", "cab", "foo" });
            var Client = GetClient();

            string Delimiter = "a";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, Keys);

            var Profixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Profixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "ba", "ca" }, Profixes);
        }

        [TestMethod("test_bucket_list_delimiter_prefix_underscore")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter and Prefix")]
        [TestProperty(MainData.Explanation, "[폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_prefix_underscore()
        {
            var BucketName = SetupObjects(new List<string>() { "_obj1_", "_under1/bar", "_under1/baz/xyzzy", "_under2/thud", "_under2/bla" });

            string Delimiter = "/";
            string Marker = "";
            string Prefix = "";

            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 1, true, new List<string>() { "_obj1_" }, EmptyList, "_obj1_");
            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, true, EmptyList, new List<string>() { "_under1/" }, "_under1/");
            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, false, EmptyList, new List<string>() { "_under2/" }, null);

            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 2, true, new List<string>() { "_obj1_" }, new List<string>() { "_under1/" }, "_under1/");
            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 2, false, EmptyList, new List<string>() { "_under2/" }, null);

            Prefix = "_under1/";

            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 1, true, new List<string>() { "_under1/bar" }, EmptyList, "_under1/bar");
            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, false, EmptyList, new List<string>() { "_under1/baz/" }, null);

            Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 2, false, new List<string>() { "_under1/bar" }, new List<string>() { "_under1/baz/" }, null);
        }

        [TestMethod("test_bucket_list_delimiter_percentage")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_percentage()
        {
            var BucketName = SetupObjects(new List<string>() { "b%ar", "b%az", "c%ab", "foo" });
            var Client = GetClient();

            string Delimiter = "%";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, Keys);

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "b%", "c%" }, Prefixes);
        }

        [TestMethod("test_bucket_list_delimiter_whitespace")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        [TestProperty(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 공백일경우 string.Empty 반환
        public void test_bucket_list_delimiter_whitespace()
        {
            var BucketName = SetupObjects(new List<string>() { "b ar", "b az", "c ab", "foo" });
            var Client = GetClient();

            string Delimiter = " ";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Delimiter));

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, Keys);

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "b ", "c " }, Prefixes);
        }

        [TestMethod("test_bucket_list_delimiter_dot")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_dot()
        {
            var BucketName = SetupObjects(new List<string>() { "b.ar", "b.az", "c.ab", "foo" });
            var Client = GetClient();

            string Delimiter = ".";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, Keys);

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "b.", "c." }, Prefixes);
        }

        [TestMethod("test_bucket_list_delimiter_unreadable")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 읽을수 없는 구분자[\\n]로 필터링 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        [TestProperty(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 \x0a일경우 string.Empty 반환
        public void test_bucket_list_delimiter_unreadable()
        {
            var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "\x0a";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Delimiter));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_delimiter_empty")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_empty()
        {
            var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter);
            Assert.IsNull(Response.Delimiter);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_delimiter_none")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_none()
        {
            var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Delimiter));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_delimiter_not_exist")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "[폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_delimiter_not_exist()
        {
            var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "/";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_delimiter_not_skip_special")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 특수문자가 생략되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)] // Default MaxKeys = 1000이라 목록을 다 읽어 올 수 없어 늘렸음
        public void test_bucket_list_delimiter_not_skip_special()
        {
            var KeyNames = new List<string>();
            for (int i = 1000; i < 1999; i++) KeyNames.Add("0/" + i.ToString());
            var KeyNames2 = new List<string>() { "1999", "1999#", "1999+", "2000" };
            KeyNames.AddRange(KeyNames2);
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "/";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter, MaxKeys: 2000);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames2, Keys);
            CollectionAssert.AreEqual(new List<string>() { "0/" }, Prefixes);
        }

        [TestMethod("test_bucket_list_prefix_basic")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "[접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void Tetest_bucket_list_prefix_basicst()
        {
            var BucketName = SetupObjects(new List<string>() { "foo/bar", "foo/baz", "quux" });
            var Client = GetClient();

            string Prefix = "foo/";
            var Response = Client.ListObjects(BucketName, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>() { "foo/bar", "foo/baz" }, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_prefix_alt")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_alt()
        {
            var BucketName = SetupObjects(new List<string>() { "bar", "baz", "foo" });
            var Client = GetClient();

            string Prefix = "ba";
            var Response = Client.ListObjects(BucketName, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>() { "bar", "baz" }, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_prefix_empty")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_empty()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Prefix = "";
            var Response = Client.ListObjects(BucketName, Prefix: Prefix);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Prefix));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_prefix_none")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_none()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Prefix));


            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_prefix_not_exist")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "[접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_not_exist()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Prefix = "d";
            var Response = Client.ListObjects(BucketName, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(0, Keys.Count);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_prefix_unreadable")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        [TestProperty(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 \x0a일경우 string.Empty 반환
        public void test_bucket_list_prefix_unreadable()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Prefix = "\x0a";
            var Response = Client.ListObjects(BucketName, Prefix: Prefix);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Prefix));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(0, Keys.Count);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_prefix_delimiter_basic")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_delimiter_basic()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Prefix = "foo/";
            string Delimiter = "/";
            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>() { "foo/bar" }, Keys);
            CollectionAssert.AreEqual(new List<string>() { "foo/baz/" }, Prefixes);
        }

        [TestMethod("test_bucket_list_prefix_delimiter_alt")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "[구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_delimiter_alt()
        {
            var KeyNames = new List<string>() { "bar", "bazar", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "a";
            string Prefix = "ba";

            var Response = Client.ListObjects(BucketName, Delimiter: Delimiter, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>() { "bar" }, Keys);
            CollectionAssert.AreEqual(new List<string>() { "baza" }, Prefixes);
        }

        [TestMethod("test_bucket_list_prefix_delimiter_prefix_not_exist")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "[입력한 접두어와 일치하는 오브젝트가 없을 경우]" +
                                     "접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_delimiter_prefix_not_exist()
        {
            var BucketName = SetupObjects(new List<string>() { "b/a/r", "b/a/c", "b/a/g", "g" });
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName, Delimiter: "d", Prefix: "/");

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(0, Keys.Count);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_prefix_delimiter_delimiter_not_exist")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "[구분자가 '/'가 아닐 경우]" +
                                     "접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_delimiter_delimiter_not_exist()
        {
            var BucketName = SetupObjects(new List<string>() { "b/a/c", "b/a/g", "b/a/r", "g" });
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName, Delimiter: "z", Prefix: "b");

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>() { "b/a/c", "b/a/g", "b/a/r" }, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "[구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우]" +
                                     "접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist()
        {
            var BucketName = SetupObjects(new List<string>() { "b/a/r", "b/a/c", "b/a/g", "g" });
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName, Delimiter: "z", Prefix: "y");

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(0, Keys.Count);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_list_maxkeys_one")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "MaxKeys")]
        [TestProperty(MainData.Explanation, "오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_maxkeys_one()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName, MaxKeys: 1);
            Assert.IsTrue(Response.IsTruncated);

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames.GetRange(0, 1), Keys);

            Response = Client.ListObjects(BucketName, Marker: KeyNames[0]);
            Assert.IsFalse(Response.IsTruncated);

            Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames.GetRange(1, KeyNames.Count - 1), Keys);
        }

        [TestMethod("test_bucket_list_maxkeys_zero")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "MaxKeys")]
        [TestProperty(MainData.Explanation, "오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_maxkeys_zero()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName, MaxKeys: 0);

            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            Assert.AreEqual(0, Keys.Count);
        }

        [TestMethod("test_bucket_list_maxkeys_none")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "MaxKeys")]
        [TestProperty(MainData.Explanation, "[default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_maxkeys_none()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName);
            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(1000, Response.MaxKeys);
        }

        [TestMethod("test_bucket_list_maxkeys_invalid")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "MaxKeys")]
        [TestProperty(MainData.Explanation, "[함수가 호출되기 전에 URL에 유효하지 않은 최대목록갯수를 추가할 경우] 오브젝트 목록 조회 실패 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_list_maxkeys_invalid()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            Client.Client.BeforeRequestEvent += BeforeCallS3ListObjectsMaxKeys;

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.ListObjects(BucketName));

            var StatusCode = e.StatusCode;
            var ErrorCode = e.ErrorCode;

            Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
            Assert.AreEqual(MainData.InvalidArgument, ErrorCode);
        }

        private void BeforeCallS3ListObjectsMaxKeys(object sender, RequestEventArgs e)
        {
            var requestEvent = e as WebServiceRequestEventArgs;
            requestEvent.ParameterCollection.Add("max-keys", "blah");
            //requestEvent.Headers.Add("max-keys", "blah");
        }

        [TestMethod("test_bucket_list_marker_none")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Marker")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_marker_none()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName, Marker: "");
            Assert.IsNull(Response.NextMarker);
        }

        [TestMethod("test_bucket_list_marker_empty")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Marker")]
        [TestProperty(MainData.Explanation, "빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_marker_empty()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjects(BucketName, Marker: "");
            Assert.IsNull(Response.NextMarker);
            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames, Keys);
        }

        [TestMethod("test_bucket_list_marker_unreadable")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Marker")]
        [TestProperty(MainData.Explanation, "마커에 읽을수 없는 값[\\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_marker_unreadable()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Marker = "\x0a";

            var Response = Client.ListObjects(BucketName, Marker: Marker);
            Assert.IsNull(Response.NextMarker);
            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames, Keys);
        }

        [TestMethod("test_bucket_list_marker_not_in_list")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Marker")]
        [TestProperty(MainData.Explanation, "[마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] " +
            "마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_marker_not_in_list()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Marker = "blah";

            var Response = Client.ListObjects(BucketName, Marker: Marker);
            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo", "quxx" }, Keys);
        }

        [TestMethod("test_bucket_list_marker_after_list")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Marker")]
        [TestProperty(MainData.Explanation, "[마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경]" +
                                     "마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_marker_after_list()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Marker = "zzz";

            var Response = Client.ListObjects(BucketName, Marker: Marker);
            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            Assert.AreEqual(0, Keys.Count);
        }

        [TestMethod("test_bucket_list_return_data")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "Metadata")]
        [TestProperty(MainData.Explanation, "ListObjcets으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_return_data()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Data = new List<ObjectData>();


            foreach (var Key in KeyNames)
            {
                var ObjResponse = Client.GetObjectMetadata(BucketName, Key);
                var ACLResponse = Client.GetObjectACL(BucketName, Key);

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

            var Response = Client.ListObjects(BucketName);
            var ObjList = Response.S3Objects;

            foreach (var Object in ObjList)
            {
                var KeyName = Object.Key;
                var KeyData = GetObjectToKey(KeyName, Data);

                Assert.IsNotNull(KeyData);
                Assert.AreEqual(KeyData.ETag, Object.ETag);
                Assert.AreEqual(KeyData.ContentLength, Object.Size);
                Assert.AreEqual(KeyData.DisplayName, Object.Owner.DisplayName);
                Assert.AreEqual(KeyData.Id, Object.Owner.Id);
                Assert.AreEqual(KeyData.LastModified, Object.LastModified.ToUniversalTime());
            }
        }

        [TestMethod("test_bucket_list_objects_anonymous")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "ACL")]
        [TestProperty(MainData.Explanation, "권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_list_objects_anonymous()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            Client.PutBucketACL(BucketName, ACL: Amazon.S3.S3CannedACL.PublicRead);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            UnauthenticatedClient.ListObjects(BucketName);
        }

        [TestMethod("test_bucket_list_objects_anonymous_fail")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "ACL")]
        [TestProperty(MainData.Explanation, "권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_list_objects_anonymous_fail()
        {
            var BucketName = GetNewBucket();
            var UnauthenticatedClient = GetUnauthenticatedClient();

            var e = Assert.ThrowsException<AmazonS3Exception>(() => UnauthenticatedClient.ListObjects(BucketName));
            var StatusCode = e.StatusCode;
            var ErrorCode = e.ErrorCode;

            Assert.AreEqual(HttpStatusCode.Forbidden, StatusCode);
            Assert.AreEqual(MainData.AccessDenied, ErrorCode);
        }

        [TestMethod("test_bucket_notexist")]
        [TestProperty(MainData.Major, "ListObjects")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_notexist()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.ListObjects(BucketName));

            var StatusCode = e.StatusCode;
            var ErrorCode = e.ErrorCode;

            Assert.AreEqual(HttpStatusCode.NotFound, StatusCode);
            Assert.AreEqual(MainData.NoSuchBucket, ErrorCode);
        }
    }
}
