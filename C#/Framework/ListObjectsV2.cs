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
using System.Collections.Generic;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Amazon.S3;

namespace s3tests2
{
    [TestClass]
    public class ListObjectsV2 : TestBase
    {
        [TestMethod("test_bucket_listv2_many")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "버킷의 오브젝트 목록을 올바르게 가져오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_many()
        {
            var BucketName = SetupObjects(new List<string>() { "foo", "bar", "baz" });
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName, MaxKeys: 2);
            CollectionAssert.AreEqual(new List<string>(){ "bar", "baz" }, GetKeys(Response));
            Assert.AreEqual(2, Response.S3Objects.Count);
            Assert.IsTrue(Response.IsTruncated);

            Response = Client.ListObjectsV2(BucketName, StartAfter: "baz", MaxKeys: 2);
            CollectionAssert.AreEqual(new List<string>(){ "foo" }, GetKeys(Response));
            Assert.AreEqual(1, Response.S3Objects.Count);
            Assert.IsFalse(Response.IsTruncated);
        }

        [TestMethod("test_basic_key_count")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Key count")]
        [TestProperty(MainData.Explanation, "ListObjectV2로 오브젝트 목록을 가져올때 Key Count 값을 올바르게 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_basic_key_count()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            for (int i = 0; i < 5; i++) Client.PutObject(BucketName, i.ToString());

            var Response = Client.ListObjectsV2(BucketName);

            Assert.AreEqual(5, Response.KeyCount);
        }

        [TestMethod("test_bucket_listv2_delimiter_basic")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_basic()
        {
            var BucketName = SetupObjects(new List<string>() { "foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf" });
            var Client = GetClient();

            string MyDelimiter = "/";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: MyDelimiter);
            Assert.AreEqual(MyDelimiter, Response.Delimiter);
            CollectionAssert.AreEqual(new List<string>(){ "asdf" }, GetKeys(Response));

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>(){ "foo/", "quux/" }, Prefixes);
        }

        [TestMethod("test_bucket_listv2_encoding_basic")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Encoding")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        [TestProperty(MainData.Different, MainData.True)]
        public void test_bucket_listv2_encoding_basic()
        {
            var BucketName = SetupObjects(new List<string>() { "foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b" });
            var Client = GetClient();

            string Delimiter = "/";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter, EncodingTypeName: "url");
            Assert.AreEqual(Delimiter, Response.Delimiter);
            CollectionAssert.AreEqual(new List<string>() { "asdf%2Bb" }, GetKeys(Response));

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(3, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "foo%2B1/", "foo/", "quux+ab/" }, Prefixes);
        }

        [TestMethod("test_bucket_listv2_delimiter_prefix")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter and Prefix")]
        [TestProperty(MainData.Explanation, "조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjcetsv2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_prefix()
        {
            var BucketName = SetupObjects(new List<string>() { "asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla" });

            string Delimiter = "/";
            string ContinuationToken = "";
            string prefix = "";

            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, null, 1, true, new List<string>() { "asdf" }, new List<string>());
            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, ContinuationToken, 1, true, EmptyList, new List<string>() { "boo/" });
            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, ContinuationToken, 1, false, EmptyList, new List<string>() { "cquux/" }, true);

            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, null, 2, true, new List<string>() { "asdf" }, new List<string>() { "boo/" });
            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, ContinuationToken, 2, false, EmptyList, new List<string>() { "cquux/" }, true);

            prefix = "boo/";

            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, null, 1, true, new List<string>() { "boo/bar" }, new List<string>());
            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, ContinuationToken, 1, false, EmptyList, new List<string>() { "boo/baz/" }, true);

            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, null, 2, false, new List<string>() { "boo/bar" }, new List<string>() { "boo/baz/" }, true);
        }

        [TestMethod("test_bucket_listv2_delimiter_prefix_ends_with_delimiter")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter and Prefix")]
        [TestProperty(MainData.Explanation, "비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_prefix_ends_with_delimiter()
        {
            var BucketName = SetupObjects(new List<string>() { "asdf/" }, Body:"");
            ValidateListObjcetV2(BucketName, "asdf/", "/", null, 1000, false, new List<string>() { "asdf/" }, EmptyList, true);
        }

        [TestMethod("test_bucket_listv2_delimiter_alt")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_alt()
        {
            var BucketName = SetupObjects(new List<string>() { "bar", "baz", "cab", "foo" });
            var Client = GetClient();

            string Delimiter = "a";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, Keys);

            var Profixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Profixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "ba", "ca" }, Profixes);
        }

        [TestMethod("test_bucket_listv2_delimiter_prefix_underscore")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter and Prefix")]
        [TestProperty(MainData.Explanation, "[폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_prefix_underscore()
        {
            var BucketName = SetupObjects(new List<string>() { "_obj1_", "_under1/bar", "_under1/baz/xyzzy", "_under2/thud", "_under2/bla" });

            string delim = "/";
            string ContinuationToken = "";
            string prefix = "";

            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, null, 1, true, new List<string>() { "_obj1_" }, new List<string>());
            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, ContinuationToken, 1, true, EmptyList, new List<string>() { "_under1/" });
            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, ContinuationToken, 1, false, EmptyList, new List<string>() { "_under2/" }, true);

            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, null, 2, true, new List<string>() { "_obj1_" }, new List<string>() { "_under1/" });
            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, ContinuationToken, 2, false, EmptyList, new List<string>() { "_under2/" }, true);

            prefix = "_under1/";

            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, null, 1, true, new List<string>() { "_under1/bar" }, new List<string>());
            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, ContinuationToken, 1, false, EmptyList, new List<string>() { "_under1/baz/" }, true);

            ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, null, 2, false, new List<string>() { "_under1/bar" }, new List<string>() { "_under1/baz/" }, true);
        }

        [TestMethod("test_bucket_listv2_delimiter_percentage")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_percentage()
        {
            var BucketName = SetupObjects(new List<string>() { "b%ar", "b%az", "c%ab", "foo" });
            var Client = GetClient();

            string Delimiter = "%";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, Keys);

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "b%", "c%" }, Prefixes);
        }

        [TestMethod("test_bucket_listv2_delimiter_whitespace")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        [TestProperty(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 공백일경우 string.Empty 반환
        public void test_bucket_listv2_delimiter_whitespace()
        {
            var BucketName = SetupObjects(new List<string>() { "b ar", "b az", "c ab", "foo" });
            var Client = GetClient();

            string Delimiter = " ";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Delimiter));

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, Keys);

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "b ", "c " }, Prefixes);
        }

        [TestMethod("test_bucket_listv2_delimiter_dot")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_dot()
        {
            var BucketName = SetupObjects(new List<string>() { "b.ar", "b.az", "c.ab", "foo" });
            var Client = GetClient();

            string Delimiter = ".";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>() { "foo" }, Keys);

            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(2, Prefixes.Count);
            CollectionAssert.AreEqual(new List<string>() { "b.", "c." }, Prefixes);
        }

        [TestMethod("test_bucket_listv2_delimiter_unreadable")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 읽을수 없는 구분자[\\n]로 필터링 되는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        [TestProperty(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 \x0a일경우 string.Empty 반환
        public void test_bucket_listv2_delimiter_unreadable()
        {
            var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "\x0a";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Delimiter));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_delimiter_empty")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_empty()
        {
            var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Delimiter));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_delimiter_none")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_none()
        {
            var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Delimiter));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_fetchowner_notempty")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Fetchowner")]
        [TestProperty(MainData.Explanation, "[권한정보를 가져오도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_fetchowner_notempty()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName, FetchOwner: true);
            var ObjectList = Response.S3Objects;

            Assert.IsNotNull(ObjectList[0].Owner);
        }

        [TestMethod("test_bucket_listv2_fetchowner_defaultempty")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Fetchowner")]
        [TestProperty(MainData.Explanation, "[default = 권한정보를 가져오지 않음] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_fetchowner_defaultempty()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName);
            var ObjectList = Response.S3Objects;

            Assert.IsNull(ObjectList[0].Owner);
        }

        [TestMethod("test_bucket_listv2_fetchowner_empty")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Fetchowner")]
        [TestProperty(MainData.Explanation, "[권한정보를 가져오지 않도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_fetchowner_empty()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName, FetchOwner: false);
            var ObjectList = Response.S3Objects;

            Assert.IsNull(ObjectList[0].Owner);
        }

        [TestMethod("test_bucket_listv2_delimiter_not_exist")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Delimiter")]
        [TestProperty(MainData.Explanation, "[폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_delimiter_not_exist()
        {
            var KeyNames = new List<string>() { "bar", "baz", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "/";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;

            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_basic")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "[접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_basic()
        {
            var BucketName = SetupObjects(new List<string>() { "foo/bar", "foo/baz", "quux" });
            var Client = GetClient();

            string Prefix = "foo/";
            var Response = Client.ListObjectsV2(BucketName, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>(){ "foo/bar", "foo/baz" }, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_alt")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_alt()
        {
            var BucketName = SetupObjects(new List<string>() { "bar", "baz", "foo" });
            var Client = GetClient();

            string Prefix = "ba";
            var Response = Client.ListObjectsV2(BucketName, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>(){ "bar", "baz" }, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_empty")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_empty()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Prefix = "";
            var Response = Client.ListObjectsV2(BucketName, Prefix: Prefix);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Prefix));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_none")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_none()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Prefix));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_not_exist")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "[접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_not_exist()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Prefix = "d";
            var Response = Client.ListObjectsV2(BucketName, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(0, Keys.Count);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_unreadable")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix")]
        [TestProperty(MainData.Explanation, "읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        [TestProperty(MainData.Different, MainData.True)]//s3 라이브러리에서 Delimiter가 \x0a일경우 string.Empty 반환
        public void test_bucket_listv2_prefix_unreadable()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz", "quux" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Prefix = "\x0a";
            var Response = Client.ListObjectsV2(BucketName, Prefix: Prefix);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.Prefix));

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(0, Keys.Count);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_delimiter_basic")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_delimiter_basic()
        {
            var KeyNames = new List<string>() { "foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Prefix = "foo/";
            string Delimiter = "/";
            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>(){ "foo/bar" }, Keys);
            CollectionAssert.AreEqual(new List<string>(){ "foo/baz/" }, Prefixes);
        }

        [TestMethod("test_bucket_listv2_prefix_delimiter_alt")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "[구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_delimiter_alt()
        {
            var KeyNames = new List<string>() { "bar", "bazar", "cab", "foo" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            string Delimiter = "a";
            string Prefix = "ba";

            var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter, Prefix: Prefix);
            Assert.AreEqual(Prefix, Response.Prefix);
            Assert.AreEqual(Delimiter, Response.Delimiter);

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>() { "bar" }, Keys);
            CollectionAssert.AreEqual(new List<string>() { "baza" }, Prefixes);
        }

        [TestMethod("test_bucket_listv2_prefix_delimiter_prefix_not_exist")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "[입력한 접두어와 일치하는 오브젝트가 없을 경우]" +
                                     " 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_delimiter_prefix_not_exist()
        {
            var BucketName = SetupObjects(new List<string>() { "b/a/r", "b/a/c", "b/a/g", "g" });
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName, Delimiter: "d", Prefix: "/");

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(0, Keys.Count);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_delimiter_delimiter_not_exist")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "[구분자가 '/'가 아닐 경우]" +
                                     "접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_delimiter_delimiter_not_exist()
        {
            var BucketName = SetupObjects(new List<string>() { "b/a/c", "b/a/g", "b/a/r", "g" });
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName, Delimiter: "z", Prefix: "b");

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            CollectionAssert.AreEqual(new List<string>() { "b/a/c", "b/a/g", "b/a/r" }, Keys);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Prefix and Delimiter")]
        [TestProperty(MainData.Explanation, "[구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우]" +
                                     "접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist()
        {
            var BucketName = SetupObjects(new List<string>() { "b/a/r", "b/a/c", "b/a/g", "g" });
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName, Delimiter: "z", Prefix: "y");

            var Keys = GetKeys(Response);
            var Prefixes = Response.CommonPrefixes;
            Assert.AreEqual(0, Keys.Count);
            Assert.AreEqual(0, Prefixes.Count);
        }

        [TestMethod("test_bucket_listv2_maxkeys_one")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "MaxKeys")]
        [TestProperty(MainData.Explanation, "오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_maxkeys_one()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName, MaxKeys: 1);
            Assert.IsTrue(Response.IsTruncated);

            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames.GetRange(0, 1), Keys);

            Response = Client.ListObjectsV2(BucketName, StartAfter: KeyNames[0]);
            Assert.IsFalse(Response.IsTruncated);

            Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames.GetRange(1, KeyNames.Count - 1), Keys);
        }

        [TestMethod("test_bucket_listv2_maxkeys_zero")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "MaxKeys")]
        [TestProperty(MainData.Explanation, "오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_maxkeys_zero()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName, MaxKeys: 0);

            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            Assert.AreEqual(0, Keys.Count);
        }

        [TestMethod("test_bucket_listv2_maxkeys_none")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "MaxKeys")]
        [TestProperty(MainData.Explanation, "[default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_maxkeys_none()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response = Client.ListObjectsV2(BucketName);
            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames, Keys);
            Assert.AreEqual(1000, Response.MaxKeys);
        }

        [TestMethod("test_bucket_listv2_continuationtoken")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Continuationtoken")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 다음 토큰값을 올바르게 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_continuationtoken()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var Response1 = Client.ListObjectsV2(BucketName, MaxKeys: 1);
            var NextContinuationToken = Response1.NextContinuationToken;

            var Response2 = Client.ListObjectsV2(BucketName, ContinuationToken: NextContinuationToken);
            Assert.AreEqual(NextContinuationToken, Response2.ContinuationToken);
            Assert.IsFalse(Response2.IsTruncated);
            var KeyNames2 = new List<string>() { "baz", "foo", "quxx" };
            var Keys = GetKeys(Response2);
            CollectionAssert.AreEqual(KeyNames2, Keys);
        }

        [TestMethod("test_bucket_listv2_both_continuationtoken_startafter")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "Continuationtoken and StartAfter")]
        [TestProperty(MainData.Explanation, "오브젝트 목록을 가져올때 Startafter와 토큰이 재대로 동작하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_both_continuationtoken_startafter()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var StartAfter = "bar";

            var Response1 = Client.ListObjectsV2(BucketName, StartAfter: StartAfter, MaxKeys: 1);
            var NextContinuationToken = Response1.NextContinuationToken;


            var Response2 = Client.ListObjectsV2(BucketName, StartAfter: StartAfter, ContinuationToken: NextContinuationToken);
            Assert.AreEqual(NextContinuationToken, Response2.ContinuationToken);
            //Assert.AreEqual(StartAfter, Response2.StartAfter);
            Assert.IsFalse(Response2.IsTruncated);
            var KeyNames2 = new List<string>() { "foo", "quxx" };
            var Keys = GetKeys(Response2);
            CollectionAssert.AreEqual(KeyNames2, Keys);
        }

        [TestMethod("test_bucket_listv2_startafter_unreadable")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "StartAfter")]
        [TestProperty(MainData.Explanation, "StartAfter에 읽을수 없는 값[\\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_startafter_unreadable()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var StartAfter = "\x0a";

            var Response = Client.ListObjectsV2(BucketName, StartAfter: StartAfter);
            Assert.IsTrue(string.IsNullOrWhiteSpace(Response.StartAfter));
            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(KeyNames, Keys);
        }

        [TestMethod("test_bucket_listv2_startafter_not_in_list")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "StartAfter")]
        [TestProperty(MainData.Explanation, "[StartAfter와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] " +
            "마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_startafter_not_in_list()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var StartAfter = "blah";

            var Response = Client.ListObjectsV2(BucketName, StartAfter: StartAfter);
            Assert.AreEqual(StartAfter, Response.StartAfter);
            var Keys = GetKeys(Response);
            CollectionAssert.AreEqual(new List<string>(){ "foo", "quxx" }, Keys);
        }

        [TestMethod("test_bucket_listv2_startafter_after_list")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "StartAfter")]
        [TestProperty(MainData.Explanation, "[StartAfter와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경]" +
                                     "StartAfter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_startafter_after_list()
        {
            var KeyNames = new List<string>() { "bar", "baz", "foo", "quxx" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var StartAfter = "zzz";

            var Response = Client.ListObjectsV2(BucketName, StartAfter: StartAfter);
            Assert.AreEqual(StartAfter, Response.StartAfter);
            Assert.IsFalse(Response.IsTruncated);
            var Keys = GetKeys(Response);
            Assert.AreEqual(0, Keys.Count);
        }

        [TestMethod("test_bucket_listv2_objects_anonymous")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "ACL")]
        [TestProperty(MainData.Explanation, "권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_listv2_objects_anonymous()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            Client.PutBucketACL(BucketName, ACL: Amazon.S3.S3CannedACL.PublicRead);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            UnauthenticatedClient.ListObjectsV2(BucketName);
        }

        [TestMethod("test_bucket_listv2_objects_anonymous_fail")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "ACL")]
        [TestProperty(MainData.Explanation, "권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_listv2_objects_anonymous_fail()
        {
            var BucketName = GetNewBucket();

            var UnauthenticatedClient = GetUnauthenticatedClient();

            var e = Assert.ThrowsException<AmazonS3Exception>(() => UnauthenticatedClient.ListObjectsV2(BucketName));

            var StatusCode = e.StatusCode;
            var ErrorCode = e.ErrorCode;

            Assert.AreEqual(HttpStatusCode.Forbidden, StatusCode);
            Assert.AreEqual(MainData.AccessDenied, ErrorCode);
        }

        [TestMethod("test_bucketv2_notexist")]
        [TestProperty(MainData.Major, "ListObjectsV2")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인(ListObjectsV2)")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_bucketv2_notexist()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.ListObjectsV2(BucketName));

            var StatusCode = e.StatusCode;
            var ErrorCode = e.ErrorCode;

            Assert.AreEqual(HttpStatusCode.NotFound, StatusCode);
            Assert.AreEqual(MainData.NoSuchBucket, ErrorCode);
        }
    }
}
