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
using Amazon.S3;
using System;
using System.Net;
using Xunit;

namespace s3tests
{
    public class ACL : TestBase
    {
        public ACL(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

        [Fact(DisplayName = "test_object_raw_get")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 오브젝트에 접근 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_raw_get()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            var Response = UnauthenticatedClient.GetObject(BucketName, Key);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
        }

        [Fact(DisplayName = "test_object_raw_get_bucket_gone")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
                                     "권한없는 사용자가 삭제된 버킷의 삭제된 오브젝트에 접근할때 에러 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_raw_get_bucket_gone()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);
            var Client = GetClient();

            Client.DeleteObject(BucketName, Key);
            Client.DeleteBucket(BucketName);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            var e = Assert.Throws<AggregateException>(()=> UnauthenticatedClient.GetObject(BucketName, Key));

            Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
            Assert.Equal(MainData.NoSuchBucket, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_delete_key_bucket_gone")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Delete")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된 오브젝트를 삭제할때 에러 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_delete_key_bucket_gone()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);
            var Client = GetClient();

            Client.DeleteObject(BucketName, Key);
            Client.DeleteBucket(BucketName);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            var e = Assert.Throws<AggregateException>(() => UnauthenticatedClient.DeleteObject(BucketName, Key));

            Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
            Assert.Equal(MainData.NoSuchBucket, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_raw_get_object_gone")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
                                     "권한없는 사용자가 삭제된 오브젝트에 접근할때 에러 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_raw_get_object_gone()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);
            var Client = GetClient();

            Client.DeleteObject(BucketName, Key);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            var e = Assert.Throws<AggregateException>(() => UnauthenticatedClient.GetObject(BucketName, Key));

            Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
            Assert.Equal(MainData.NoSuchKey, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_raw_get_bucket_acl")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = private, Object_ACL = public-read] " +
                                     "권한없는 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_raw_get_bucket_acl()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.Private, S3CannedACL.PublicRead, Key);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            var Response = UnauthenticatedClient.GetObject(BucketName, Key);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
        }

        [Fact(DisplayName = "test_object_raw_get_object_acl")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = private] " +
                                     "권한없는 사용자가 공용버킷의 개인 오브젝트에 접근할때 에러확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_raw_get_object_acl()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.Private, Key);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            var e = Assert.Throws<AggregateException>(() => UnauthenticatedClient.GetObject(BucketName, Key));

            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
            Assert.Equal(MainData.AccessDenied, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_raw_authenticated")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
                                     "로그인한 사용자가 공용 버킷의 공용 오브젝트에 접근 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_raw_authenticated()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);

            var Client = GetClient();
            var Response = Client.GetObject(BucketName, Key);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
        }

        [Fact(DisplayName = "test_object_raw_response_headers")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Header")]
        [Trait(MainData.Explanation, "[Bucket_ACL = priavte, Object_ACL = priavte] " +
            "로그인한 사용자가 GetObject의 반환헤더값을 설정하고 개인 오브젝트를 가져올때 반환헤더값이 적용되었는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_raw_response_headers()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.Private, S3CannedACL.Private, Key);
            var Client = GetClient();

            var Response = Client.GetObject(BucketName, Key, 
                ResponseCacheControl: "no-cache", 
                ResponseContentDisposition: "bla", 
                ResponseContentEncoding: "aaa", 
                ResponseContentLanguage: "esperanto", 
                ResponseContentType: "foo/bar", 
                ResponseExpires: "123");

            
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
            Assert.Equal("foo/bar", Response.Headers.ContentType);
            Assert.Equal("bla", Response.Headers.ContentDisposition);
            Assert.Equal("aaa", Response.Headers.ContentEncoding);
            Assert.Equal("no-cache", Response.Headers.CacheControl);
        }

        [Fact(DisplayName = "test_object_raw_authenticated_bucket_acl")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = private, Object_ACL = public-read] " +
                                     "로그인한 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_raw_authenticated_bucket_acl()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.Private, S3CannedACL.PublicRead, Key);

            var Client = GetClient();
            var Response = Client.GetObject(BucketName, Key);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
        }

        [Fact(DisplayName = "test_object_raw_authenticated_object_acl")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = private] " +
                                     "로그인한 사용자가 공용버킷의 개인 오브젝트에 접근 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_raw_authenticated_object_acl()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.Private, Key);

            var Client = GetClient();
            var Response = Client.GetObject(BucketName, Key);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
        }

        [Fact(DisplayName = "test_object_raw_authenticated_bucket_gone")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
                                     "로그인한 사용자가 삭제된 버킷의 삭제된 오브젝트에 접근할때 에러 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_raw_authenticated_bucket_gone()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);
            var Client = GetClient();

            Client.DeleteObject(BucketName, Key);
            Client.DeleteBucket(BucketName);

            var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, Key));

            Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
            Assert.Equal(MainData.NoSuchBucket, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_raw_authenticated_object_gone")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
                                     "로그인한 사용자가 삭제된 오브젝트에 접근할때 에러 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_raw_authenticated_object_gone()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);
            var Client = GetClient();

            Client.DeleteObject(BucketName, Key);

            var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, Key));

            Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
            Assert.Equal(MainData.NoSuchKey, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_raw_get_x_amz_expires_not_expired")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Post")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
            "로그인이 만료되지 않은 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_raw_get_x_amz_expires_not_expired()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);
            var Client = GetClient();

            var URL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
            var Response = GetObject(URL);

            Assert.Equal(HttpStatusCode.OK, Response.StatusCode);
            Response.Close();
        }

        [Fact(DisplayName = "test_object_raw_get_x_amz_expires_out_range_zero")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Post")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
            "로그인이 만료된 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_raw_get_x_amz_expires_out_range_zero()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);
            var Client = GetClient();
            var URL = Client.GeneratePresignedURL(BucketName, Key, DateTime.MinValue, HttpVerb.GET);

            var e = Assert.Throws<WebException>(() => GetObject(URL));
            var Response = e.Response as HttpWebResponse;
            
            Assert.Equal(HttpStatusCode.Forbidden, Response.StatusCode);
        }

        [Fact(DisplayName = "test_object_raw_get_x_amz_expires_out_positive_range")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Post")]
        [Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
            "로그인 유효주기가 만료된 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_raw_get_x_amz_expires_out_positive_range()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, Key);
            var Client = GetClient();

            var URL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddDays(-1), HttpVerb.GET);
            var e = Assert.Throws<WebException>(() => GetObject(URL));
            var Response = e.Response as HttpWebResponse;
            Assert.Equal(HttpStatusCode.Forbidden, Response.StatusCode);
        }

        [Fact(DisplayName = "test_object_anon_put")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Update")]
        [Trait(MainData.Explanation, "[Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를 권한없는 사용자가 업데이트하려고 할때 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_anon_put()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "foo";

            Client.PutObject(BucketName, Key);

            var UnauthenticatedClient = GetUnauthenticatedClient();

            var e = Assert.Throws<AggregateException>(() => UnauthenticatedClient.PutObject(BucketName, Key, Body: "bar"));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
            Assert.Equal(MainData.AccessDenied, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_anon_put_write_access")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Update")]
        [Trait(MainData.Explanation, "로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를 권한없는 사용자가 업데이트하려고 할때 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_anon_put_write_access()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "foo";

            Client.PutObject(BucketName, Key);

            var UnauthenticatedClient = GetUnauthenticatedClient();

            var e = Assert.Throws<AggregateException>(() => UnauthenticatedClient.PutObject(BucketName, Key, Body: "bar"));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
            Assert.Equal(MainData.AccessDenied, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_put_authenticated")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Default")]
        [Trait(MainData.Explanation, "[Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_put_authenticated()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            var Response = Client.PutObject(BucketName, Key: "foo", Body: "foo");
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
        }

        [Fact(DisplayName = "test_object_raw_put_authenticated_expired")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Default")]
        [Trait(MainData.Explanation, "[Bucket_ACL = Default, Object_ACL = Default] " +
                                     "Post방식으로 만료된 로그인 정보를 설정하여 오브젝트 업로드 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_raw_put_authenticated_expired()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "foo";
            Client.PutObject(BucketName, Key: Key);

            var URL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddDays(-1), HttpVerb.PUT);
            var e = Assert.Throws<WebException>(() => PutObject(URL));
            var Response = e.Response as HttpWebResponse;
            Assert.Equal(HttpStatusCode.Forbidden, Response.StatusCode);
        }

        [Fact(DisplayName = "test_acl_private_bucket_public_read_object")]
        [Trait(MainData.Major, "ACL")]
        [Trait(MainData.Minor, "Get")]
        [Trait(MainData.Explanation, "[Bucket_ACL = private, Object_ACL = public-read] 모든 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_acl_private_bucket_public_read_object()
        {
            var Key = "foo";
            var BucketName = SetupBucketObjectACL(S3CannedACL.Private, S3CannedACL.PublicRead, Key);

            var Client = GetClient();
            TestACL(BucketName, Key, Client, true);

            var AltClient = GetAltClient();
            TestACL(BucketName, Key, AltClient, true);

            var UnauthenticatedClient = GetUnauthenticatedClient();
            TestACL(BucketName, Key, UnauthenticatedClient, true);
        }
    }
}
