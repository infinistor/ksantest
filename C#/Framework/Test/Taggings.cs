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
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Amazon.S3;
using Newtonsoft.Json.Linq;
using System.Text;

namespace s3tests2
{
    [TestClass]
    public class Taggings : TestBase
    {
        [TestMethod("test_set_tagging")]
        [TestProperty(MainData.Major, "Tagging")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "버킷에 사용자 추가 태그값을 설정할경우 성공확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_set_tagging()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            var Tags = new List<Tag>() {
                new Tag()
                {
                Key = "Hello",
                Value = "world"
                }
            };

            var Response = Client.GetBucketTagging(BucketName);
            Assert.AreEqual(0, Response.TagSet.Count);

            Client.PutBucketTagging(BucketName, Tags);

            Response = Client.GetBucketTagging(BucketName);
            Assert.AreEqual(1, Response.TagSet.Count);
            Assert.AreEqual(Tags[0].Key, Response.TagSet[0].Key);
            Assert.AreEqual(Tags[0].Value, Response.TagSet[0].Value);

            Client.DeleteBucketTagging(BucketName);

            Response = Client.GetBucketTagging(BucketName);
            Assert.AreEqual(0, Response.TagSet.Count);

        }
        [TestMethod("test_get_obj_tagging")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "오브젝트에 태그 설정이 올바르게 적용되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_get_obj_tagging()
        {
            var Key = "testputtags";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var InputTagSet = MakeSimpleTagset(2);

            var PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagSet);
            Assert.AreEqual(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);
        }

        [TestMethod("test_get_obj_head_tagging")]
        [Ignore("AWS에서는 헤더에서 Tag갯수를 반환하지 않음")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "오브젝트에 태그 설정이 올바르게 적용되는지 헤더정보를 통해 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_get_obj_head_tagging()
        {
            var Key = "testputtags";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();
            var Count = 2;

            var InputTagSet = MakeSimpleTagset(Count);

            var PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagSet);
            Assert.AreEqual(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var GetResponse = Client.GetObject(BucketName, Key);
            Assert.AreEqual(HttpStatusCode.OK, GetResponse.HttpStatusCode);
            Assert.AreEqual(Count.ToString(), GetResponse.Headers["x-amz-tagging-count"]);
        }

        [TestMethod("test_put_max_tags")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Max")]
        [TestProperty(MainData.Explanation, "추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_put_max_tags()
        {
            var Key = "testputmaxtags";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var InputTagSet = MakeSimpleTagset(10);

            var PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagSet);
            Assert.AreEqual(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);
        }

        [TestMethod("test_put_excess_tags")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Overflow")]
        [TestProperty(MainData.Explanation, "추가가능한 최대갯수를 넘겨서 태그를 입력할때 에러 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_put_excess_tags()
        {
            var Key = "testputmaxtags";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var InputTagSet = MakeSimpleTagset(11);

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutObjectTagging(BucketName, Key, InputTagSet));
            var StatusCode = e.StatusCode;
            var ErrorCode = e.ErrorCode;
            Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
            Assert.AreEqual(MainData.BadRequest, ErrorCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            Assert.AreEqual(0, GetResponse.Tagging.Count);
        }

        [TestMethod("test_put_max_kvsize_tags")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Max")]
        [TestProperty(MainData.Explanation, "태그의 key값의 길이가 최대(128) value값의 길이가 최대(256)일때 태그를 입력할 수 있는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_put_max_kvsize_tags()
        {
            var Key = "testputmaxkeysize";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var InputTagSet = new Tagging() { TagSet = MakeTagList(128, 256) };
            
            var PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagSet);
            Assert.AreEqual(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);
        }

        [TestMethod("test_put_excess_key_tags")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Overflow")]
        [TestProperty(MainData.Explanation, "태그의 key값의 길이가 최대(129) value값의 길이가 최대(256)일때 태그 입력 실패 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_put_excess_key_tags()
        {
            var Key = "testputexcesskeytags";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var InputTagSet = new Tagging() { TagSet = MakeTagList(129, 256) };

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutObjectTagging(BucketName, Key, InputTagSet));
            var StatusCode = e.StatusCode;
            var ErrorCode = e.ErrorCode;
            Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
            Assert.AreEqual(MainData.InvalidTag, ErrorCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            Assert.AreEqual(0, GetResponse.Tagging.Count);
        }

        [TestMethod("test_put_excess_val_tags")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Overflow")]
        [TestProperty(MainData.Explanation, "태그의 key값의 길이가 최대(128) value값의 길이가 최대(257)일때 태그 입력 실패 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
        public void test_put_excess_val_tags()
        {
            var Key = "testputexcesskeytags";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var InputTagSet = new Tagging() { TagSet = MakeTagList(128, 257) };

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutObjectTagging(BucketName, Key, InputTagSet));
            var StatusCode = e.StatusCode;
            var ErrorCode = e.ErrorCode;
            Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
            Assert.AreEqual(MainData.InvalidTag, ErrorCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            Assert.AreEqual(0, GetResponse.Tagging.Count);
        }

        [TestMethod("test_put_modify_tags")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Overwrite")]
        [TestProperty(MainData.Explanation, "오브젝트의 태그목록을 덮어쓰기 가능한지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_put_modify_tags()
        {
            var Key = "testputmodifytags";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var InputTagSet = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag(){ Key = "key", Value = "val"},
                    new Tag(){ Key = "key2", Value = "val2"},
                }
            };

            var PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagSet);
            Assert.AreEqual(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);

            var InputTagSet2 = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag(){ Key = "key3", Value = "val3"},
                }
            };

            PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagSet2);
            Assert.AreEqual(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            GetResponse = Client.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagSet2.TagSet, GetResponse.Tagging);
        }

        [TestMethod("test_put_delete_tags")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Delete")]
        [TestProperty(MainData.Explanation, "오브젝트의 태그를 삭제 가능한지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_put_delete_tags()
        {
            var Key = "testputmodifytags";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var InputTagSet = MakeSimpleTagset(2);

            var PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagSet);
            Assert.AreEqual(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);

            var DelResponse = Client.DeleteObjectTagging(BucketName, Key);
            Assert.AreEqual(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);

            GetResponse = Client.GetObjectTagging(BucketName, Key);
            Assert.AreEqual(0, GetResponse.Tagging.Count);
        }

        [TestMethod("test_post_object_tags_authenticated_request")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "Post")]
        [TestProperty(MainData.Explanation, "로그인 정보가 있는 Post방식으로 태그정보, ACL을 포함한 오브젝트를 업로드 가능한지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_post_object_tags_authenticated_request()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var ContentType = "text/plain";
            var Key = "foo.txt";

            var InputTagSet = MakeSimpleTagset(2);
            var XmlInputTagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";

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
                        { new JArray() { "starts-with", "$tagging", "" } },
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
                    { "tagging", XmlInputTagset },
                    { "Content-Type", ContentType },
                    { "file", FileData },
            };

            var Result = PostUpload(BucketName, Payload);
            Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

            var Response = Client.GetObject(BucketName, Key);
            var Body = GetBody(Response);
            Assert.AreEqual("bar", Body);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);
        }

        [TestMethod("test_put_obj_with_tags")]
        [TestProperty(MainData.Major, "Taggings")]
        [TestProperty(MainData.Minor, "PutObject")]
        [TestProperty(MainData.Explanation, "헤더에 태그정보를 포함한 오브젝트 업로드 성공 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_put_obj_with_tags()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "testtagobj1";
            var Data = RandomTextToLong(100);

            var TagSet = new List<Tag>()
            {
                new Tag(){ Key = "bar", Value = ""},
                new Tag(){ Key = "foo", Value = "bar"},
            };

            var Headers = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-tagging", "foo=bar&bar") };

            Client.PutObject(BucketName, Key, Body: Data, HeaderList: Headers);
            var Response = Client.GetObject(BucketName, Key);
            var Body = GetBody(Response);
            Assert.AreEqual(Data, Body);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            var ResponseTagSet = GetResponse.Tagging;

            TaggingCompare(TagSet, ResponseTagSet);
        }
    }
}
