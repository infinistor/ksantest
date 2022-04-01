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
using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace s3tests2
{
    [TestClass]
    public class Post : TestBase
    {
        [TestMethod("test_post_object_anonymous_request")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "post 방식으로 권한없는 사용자가 파일 업로드할 경우 성공 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_anonymous_request()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();

            Client.PutBucket(BucketName, ACL: S3CannedACL.PublicReadWrite);

            var ContentType = "text/plain";
            var FileData = new FormFile() { Name = "foo.txt", ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", "foo.txt" },
                    { "acl", "public-read" },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

            var Response = Client.GetObject(BucketName, "foo.txt");
            var Body = GetBody(Response);
            Assert.AreEqual("bar", Body);
        }

        [TestMethod("test_post_object_authenticated_request")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "post 방식으로 로그인 정보를 포함한 파일 업로드할 경우 성공 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_authenticated_request()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

            var Response = Client.GetObject(BucketName, Key);
            var Body = GetBody(Response);
            Assert.AreEqual("bar", Body);
        }

        [TestMethod("test_post_object_authenticated_no_content_type")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "content-type 헤더 정보 없이 post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_authenticated_no_content_type()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ACL: S3CannedACL.PublicReadWrite);
            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);
            var Response = Client.GetObject(BucketName, Key);
            var Body = GetBody(Response);
            Assert.AreEqual("bar", Body);
        }

        [TestMethod("test_post_object_authenticated_request_bad_access_key")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[AccessKey 값이 틀린 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_authenticated_request_bad_access_key()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ACL: S3CannedACL.PublicReadWrite);

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", "foo" },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.Forbidden, Result.StatusCode);
        }

        [TestMethod("test_post_object_set_success_code")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Status code")]
        [TestProperty(MainData.Explanation, "[성공시 반환상태값을 201로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_set_success_code()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();

            Client.PutBucket(BucketName, ACL: S3CannedACL.PublicReadWrite);

            var ContentType = "text/plain";
            var FileData = new FormFile() { Name = "foo.txt", ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", "foo.txt" },
                    { "acl", "public-read" },
                    {"success_action_status" , "201" },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.Created, Result.StatusCode);

            var Response = Client.GetObject(BucketName, "foo.txt");
            var Body = GetBody(Response);
            Assert.AreEqual("bar", Body);
        }

        [TestMethod("test_post_object_set_invalid_success_code")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Status code")]
        [TestProperty(MainData.Explanation, "[성공시 반환상태값을 에러코드인 404로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_set_invalid_success_code()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ACL: S3CannedACL.PublicReadWrite);

            var ContentType = "text/plain";
            var FileData = new FormFile() { Name = "foo.txt", ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", "foo.txt" },
                    { "acl", "public-read" },
                    {"success_action_status" , "404" },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

            var Response = Client.GetObject(BucketName, "foo.txt");
            var Body = GetBody(Response);
            Assert.AreEqual("bar", Body);
        }

        [TestMethod("test_post_object_upload_larger_than_chunk")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "post 방식으로 로그인정보를 포함한 대용량 파일 업로드시 올바르게 업로드 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_upload_larger_than_chunk()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            var ContentType = "text/plain";
            var Key = "foo.txt";
            var Size = 5 * 1024 * 1024;
            var Data = RandomTextToLong(Size);

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, Size } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = Data };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

            var Response = Client.GetObject(BucketName, Key);
            var Body = GetBody(Response);
            Assert.AreEqual(Data, Body);
        }

        [TestMethod("test_post_object_set_key_from_filename")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "[오브젝트 이름을 로그인정보에 포함되어 있는 key값으로 대체할 경우] " +
                                     "post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_set_key_from_filename()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

            var Response = Client.GetObject(BucketName, Key);
            var Body = GetBody(Response);
            Assert.AreEqual("bar", Body);
        }

        [TestMethod("test_post_object_ignored_header")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "post 방식으로 로그인, 헤더 정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_ignored_header()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "x-ignore-foo" , "bar"},
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);
        }

        [TestMethod("test_post_object_case_insensitive_condition_fields")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "[헤더정보에 대소문자를 섞어서 사용할 경우] " +
                                     "post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_case_insensitive_condition_fields()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bUcKeT", BucketName } } },
                        { new JArray() { "StArTs-WiTh", "$KeY", "foo" } },
                        { new JObject() { { "AcL", "private" } } },
                        { new JArray() { "StArTs-WiTh", "$CoNtEnT-TyPe", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "kEy", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "aCl", "private" },
                    { "signature", Signature },
                    { "pOLICy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);
        }

        [TestMethod("test_post_object_escaped_field_values")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "[오브젝트 이름에 '\'를 사용할 경우] " +
                                     "post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_escaped_field_values()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            var ContentType = "text/plain";
            var Key = "\\$foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "\\$foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

            var Response = Client.GetObject(BucketName, Key);
            var Body = GetBody(Response);
            Assert.AreEqual("bar", Body);
        }

        [TestMethod("test_post_object_success_redirect_action")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Upload")]
        [TestProperty(MainData.Explanation, "[redirect url설정하여 체크] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_success_redirect_action()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ACL: S3CannedACL.PublicReadWrite);

            var ContentType = "text/plain";
            var Key = "foo.txt";
            var RedirectURL = GetURL(BucketName);

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "eq", "$success_action_redirect", RedirectURL } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "success_action_redirect" , RedirectURL },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.OK, Result.StatusCode);

            var Response = Client.GetObject(BucketName, Key);
            Assert.AreEqual(string.Format("{0}/?bucket={1}&key={2}&etag=%22{3}%22", RedirectURL, BucketName, Key, Response.ETag.Replace("\"", "")), Result.URL);
        }

        [TestMethod("test_post_object_invalid_signature")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[SecretKey Hash 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_invalid_signature()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "\\$foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "\\$foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature.Substring(0, Signature.Length - 1) },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.Forbidden, Result.StatusCode);
        }

        [TestMethod("test_post_object_invalid_access_key")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[AccessKey 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_invalid_access_key()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "\\$foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "\\$foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey.Substring(0, Config.MainUser.AccessKey.Length - 1) },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.Forbidden, Result.StatusCode);
        }

        [TestMethod("test_post_object_invalid_date_format")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[로그인 정보의 날짜포맷이 다를경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_invalid_date_format()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "\\$foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.Now.AddMinutes(100) },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "\\$foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_no_key_specified")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[오브젝트 이름을 입력하지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_no_key_specified()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = "", ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_missing_signature")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[signature 정보를 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_missing_signature()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "\\$foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "\\$foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_missing_policy_condition")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy에 버킷 이름을 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_missing_policy_condition()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.Forbidden, Result.StatusCode);
        }

        [TestMethod("test_post_object_user_specified_header")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "Metadata")]
        [TestProperty(MainData.Explanation, "[사용자가 추가 메타데이터를 입력한 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_user_specified_header()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                        { new JArray() { "starts-with", "$x-amz-meta-foo", "bar" } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "x-amz-meta-foo" , "barclamp" },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

            var Response = Client.GetObject(BucketName, Key);
            Assert.AreEqual("barclamp", Response.Metadata["foo"]);
        }

        [TestMethod("test_post_object_request_missing_policy_specified_field")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[사용자가 추가 메타데이터를 policy에 설정하였으나 오브젝트에 해당 정보가 누락된 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_request_missing_policy_specified_field()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                        { new JArray() { "starts-with", "$x-amz-meta-foo", "bar" } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.Forbidden, Result.StatusCode);
        }

        [TestMethod("test_post_object_condition_is_case_sensitive")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy의 condition을 대문자(CONDITIONS)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_condition_is_case_sensitive()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "CONDITIONS", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_expires_is_case_sensitive")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy의 expiration을 대문자(EXPIRATION)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_expires_is_case_sensitive()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"EXPIRATION", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_expired_policy")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy의 expiration을 만료된 값으로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_expired_policy()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(-100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.Forbidden, Result.StatusCode);
        }

        [TestMethod("test_post_object_invalid_request_field_value")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[사용자가 추가 메타데이터를 policy에 설정하였으나 설정정보가 올바르지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_invalid_request_field_value()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                        { new JArray() { "eq", "$x-amz-meta-foo", "" } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "x-amz-meta-foo" , "barclamp" },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.Forbidden, Result.StatusCode);
        }

        [TestMethod("test_post_object_missing_expires_condition")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy의 expiration값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_missing_expires_condition()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_missing_conditions_list")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy의 conditions값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_missing_conditions_list()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_upload_size_limit_exceeded")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy에 설정한 용량보다 큰 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_upload_size_limit_exceeded()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0, 0 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_missing_content_length_argument")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy에 용량정보 설정을 누락할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_missing_content_length_argument()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 0 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_invalid_content_length_argument")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy에 용량정보 설정값이 틀렸을 경우(용량값을 음수로 입력)] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_invalid_content_length_argument()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", -1, 0 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_upload_size_below_minimum")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy에 설정한 용량보다 작은 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_upload_size_below_minimum()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                    {
                        { new JObject() { { "bucket", BucketName } } },
                        { new JArray() { "starts-with", "$key", "foo" } },
                        { new JObject() { { "acl", "private" } } },
                        { new JArray() { "starts-with", "$Content-Type", ContentType } },
                        { new JArray() { "content-length-range", 512, 1024 } },
                    }
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        [TestMethod("test_post_object_empty_conditions")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "[policy의 conditions값이 비어있을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_post_object_empty_conditions()
        {
            var BucketName = GetNewBucket();

            var ContentType = "text/plain";
            var Key = "foo.txt";

            var PolicyDocument = new JObject()
            {
                {"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
                { "conditions", new JArray()
                },
            };

            var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
            var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

            var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
            var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
            var Payload = new Dictionary<string, object>() {
                    { "key", Key },
                    { "AWSAccessKeyId", Config.MainUser.AccessKey },
                    { "acl", "private" },
                    { "signature", Signature },
                    { "policy", Policy },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.BadRequest, Result.StatusCode);
        }

        //[TestMethod("test_100_continue")]
        //[TestProperty(MainData.Major, "Post")]
        //[TestProperty(MainData.Minor, "100-continue")]
        //[TestProperty(MainData.Explanation, "로그인 정보없이 응답코드 성공확인")]
        //[TestProperty(MainData.Result, MainData.ResultSuccess)]
        //public void test_100_continue()
        //{
        //    var BucketName = GetNewBucket();
        //    var Client = GetClient();
        //    var ObjectName = "testobj";

        //    var StatusCode = SimpleHttpReq100Cont(ObjectName, ObjectName);
        //    Assert.AreEqual(HttpStatusCode.Forbidden, StatusCode);

        //    Client.PutBucketACL(BucketName, ACL: S3CannedACL.PublicReadWrite);

        //    StatusCode = SimpleHttpReq100Cont(ObjectName, ObjectName);
        //    Assert.AreEqual(HttpStatusCode.Continue, StatusCode);
        //}

        [TestMethod("test_presignedurl_put_get")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "PresignedURL")]
        [TestProperty(MainData.Explanation, "PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_presignedurl_put_get()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "foo";

            var PutURL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.PUT);
            var PutResponse = PutObject(PutURL, Key);

            Assert.AreEqual(HttpStatusCode.OK, PutResponse.StatusCode);
            PutResponse.Close();

            var GetURL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
            var GetResponse = GetObject(GetURL);

            Assert.AreEqual(HttpStatusCode.OK, GetResponse.StatusCode);
            GetResponse.Close();
        }


        [TestMethod("test_presignedurl_put_get_v4")]
        [TestProperty(MainData.Major, "Post")]
        [TestProperty(MainData.Minor, "PresignedURL")]
        [TestProperty(MainData.Explanation, "[SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_presignedurl_put_get_v4()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClientV4();
            var Key = "foo";

            Client.PutBucket(BucketName);

            var PutURL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.PUT);
            var PutResponse = PutObject(PutURL, Key);

            Assert.AreEqual(HttpStatusCode.OK, PutResponse.StatusCode);
            PutResponse.Close();

            var GetURL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
            var GetResponse = GetObject(GetURL);

            Assert.AreEqual(HttpStatusCode.OK, GetResponse.StatusCode);
            GetResponse.Close();
        }
    }
}
