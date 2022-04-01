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
using Amazon.S3.Model;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
{
    public class Policy : TestBase
    {
        public Policy(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

        [Fact(DisplayName = "test_bucket_policy")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Check")]
        [Trait(MainData.Explanation, "버킷에 정책 설정이 올바르게 적용되는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_policy()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "asdf";
            Client.PutObject(BucketName, Key, Body: Key);

            var Resource1 = "arn:aws:s3:::" + BucketName;
            var Resource2 = "arn:aws:s3:::" + BucketName + "/*";

            var PolicyDocument = new JObject()
            {
                {MainData.PolicyVersion, MainData.PolicyVersionDate },
                { MainData.PolicyStatement, new JArray()
                {
                     new JObject(){ { MainData.PolicyEffect, "Allow" },
                                    { MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
                                    { MainData.PolicyAction, "s3:ListBucket" },
                                    { MainData.PolicyResource, new JArray() { Resource1, Resource2 } },
                     },
                    }
                },
            };

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var AltClient = GetAltClient();
            var Response = AltClient.ListObjects(BucketName);
            Assert.Single(Response.S3Objects);

            var GetResponse = Client.GetBucketPolicy(BucketName);
            Assert.Equal(PolicyDocument.ToString().Replace("\r\n", "").Replace(" ", ""), GetResponse.Policy.Replace("\r\n", "").Replace(" ", ""));
        }

        [Fact(DisplayName = "test_bucketv2_policy")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Check")]
        [Trait(MainData.Explanation, "버킷에 정책 설정이 올바르게 적용되는지 확인(ListObjectsV2)")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_bucketv2_policy()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "asdf";
            Client.PutObject(BucketName, Key, Body: Key);

            var Resource1 = "arn:aws:s3:::" + BucketName;
            var Resource2 = "arn:aws:s3:::" + BucketName + "/*";

            var PolicyDocument = new JObject()
            {
                {MainData.PolicyVersion, MainData.PolicyVersionDate },
                { MainData.PolicyStatement, new JArray()
                {
                     new JObject(){ { MainData.PolicyEffect, "Allow" },
                                    { MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
                                    { MainData.PolicyAction, "s3:ListBucket" },
                                    { MainData.PolicyResource, new JArray() { Resource1, Resource2 } },
                     },
                    }
                },
            };

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var AltClient = GetAltClient();
            var Response = AltClient.ListObjectsV2(BucketName);
            Assert.Single(Response.S3Objects);

            var GetResponse = Client.GetBucketPolicy(BucketName);
            Assert.Equal(PolicyDocument.ToString().Replace("\r\n", "").Replace(" ", ""), GetResponse.Policy.Replace("\r\n", "").Replace(" ", ""));
        }

        [Fact(DisplayName = "test_bucket_policy_acl")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Priority")]
        [Trait(MainData.Explanation, "버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_bucket_policy_acl()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "asdf";
            Client.PutObject(BucketName, Key, Body: Key);

            var Resource1 = "arn:aws:s3:::" + BucketName;
            var Resource2 = "arn:aws:s3:::" + BucketName + "/*";

            var PolicyDocument = new JObject()
            {
                {MainData.PolicyVersion, MainData.PolicyVersionDate },
                { MainData.PolicyStatement, new JArray()
                {
                     new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectDeny },
                                    { MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
                                    { MainData.PolicyAction, "s3:ListBucket" },
                                    { MainData.PolicyResource, new JArray() { Resource1, Resource2 } },
                     },
                    }
                },
            };
            Client.PutBucketACL(BucketName, ACL: S3CannedACL.AuthenticatedRead);
            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var AltClient = GetAltClient();
            var e = Assert.Throws<AggregateException>(() => AltClient.ListObjects(BucketName));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
            Assert.Equal(MainData.AccessDenied, GetErrorCode(e));

            Client.DeleteBucketPolicy(BucketName);
            Client.PutBucketACL(BucketName, ACL: S3CannedACL.PublicRead);
        }

        [Fact(DisplayName = "test_bucketv2_policy_acl")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Priority")]
        [Trait(MainData.Explanation, "버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인(ListObjectsV2)")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_bucketv2_policy_acl()
        {
            
            var BucketName = GetNewBucket();
            var Client = GetClient();
            var Key = "asdf";
            Client.PutObject(BucketName, Key, Body: Key);

            var Resource1 = "arn:aws:s3:::" + BucketName;
            var Resource2 = "arn:aws:s3:::" + BucketName + "/*";

            var PolicyDocument = new JObject()
            {
                {MainData.PolicyVersion, MainData.PolicyVersionDate },
                { MainData.PolicyStatement, new JArray()
                {
                     new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectDeny },
                                    { MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
                                    { MainData.PolicyAction, "s3:ListBucket" },
                                    { MainData.PolicyResource, new JArray() { Resource1, Resource2 } },
                     },
                    }
                },
            };
            Client.PutBucketACL(BucketName, ACL: S3CannedACL.AuthenticatedRead);
            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var AltClient = GetAltClient();
            var e = Assert.Throws<AggregateException>(() => AltClient.ListObjectsV2(BucketName));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
            Assert.Equal(MainData.AccessDenied, GetErrorCode(e));

            Client.DeleteBucketPolicy(BucketName);
            Client.PutBucketACL(BucketName, ACL: S3CannedACL.PublicRead);
        }

        [Fact(DisplayName = "test_get_tags_acl_public")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Taggings")]
        [Trait(MainData.Explanation, "정책설정으로 오브젝트의 태그목록 읽기를 public-read로 설정했을때 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_get_tags_acl_public()
        {
            var Key = "testgettagsacl";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, Key));
            var PolicyDocument = MakeJsonPolicy("s3:GetObjectTagging", Resource);

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var InputTagset = MakeSimpleTagset(10);
            var PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagset);
            Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var AltClient = GetAltClient();

            var GetResponse = AltClient.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagset.TagSet, GetResponse.Tagging);
        }

        [Fact(DisplayName = "test_put_tags_acl_public")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Tagging")]
        [Trait(MainData.Explanation, "정책설정으로 오브젝트의 태그 입력을 public-read로 설정했을때 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_put_tags_acl_public()
        {
            var Key = "testputtagsacl";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, Key));
            var PolicyDocument = MakeJsonPolicy("s3:PutObjectTagging", Resource);

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var InputTagset = MakeSimpleTagset(10);
            var AltClient = GetAltClient();
            var PutResponse = AltClient.PutObjectTagging(BucketName, Key, InputTagset);
            Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            TaggingCompare(InputTagset.TagSet, GetResponse.Tagging);
        }

        [Fact(DisplayName = "test_delete_tags_obj_public")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Tagging")]
        [Trait(MainData.Explanation, "정책설정으로 오브젝트의 태그 삭제를 public-read로 설정했을때 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_delete_tags_obj_public()
        {
            var Key = "testdeltagsacl";
            var BucketName = SetupKeyWithRandomContent(Key);
            var Client = GetClient();

            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, Key));
            var PolicyDocument = MakeJsonPolicy("s3:DeleteObjectTagging", Resource);

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var InputTagset = MakeSimpleTagset(10);
            var PutResponse = Client.PutObjectTagging(BucketName, Key, InputTagset);
            Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

            var AltClient = GetAltClient();
            var DelResponse = AltClient.DeleteObjectTagging(BucketName, Key);
            Assert.Equal(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);

            var GetResponse = Client.GetObjectTagging(BucketName, Key);
            Assert.Empty(GetResponse.Tagging);
        }

        [Fact(DisplayName = "test_bucket_policy_get_obj_existing_tag")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Tag Options")]
        [Trait(MainData.Explanation, "[오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObject허용]" +
                                     "조건부 정책설정시 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_policy_get_obj_existing_tag()
        {
            var publictag = "publictag";
            var privatetag = "privatetag";
            var invalidtag = "invalidtag";
            var BucketName = SetupObjects(new List<string>() { publictag, privatetag, invalidtag });
            var Client = GetClient();

            var TagConditional = new JObject() {
                { "StringEquals",
                    new JObject() {
                        { "s3:ExistingObjectTag/security", "public" }
                    }
                }
            };

            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, "*"));
            var PolicyDocument = MakeJsonPolicy("s3:GetObject", Resource, Conditions: TagConditional);

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());
            
            var InputTagset = new Tagging() {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="public" },
                    new Tag() { Key= "foo", Value="bar" },
                },
            };
            var Response = Client.PutObjectTagging(BucketName, publictag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="private" },
                },
            };
            Response = Client.PutObjectTagging(BucketName, privatetag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security1", Value="public" },
                },
            };
            Response = Client.PutObjectTagging(BucketName, invalidtag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            var AltClient = GetAltClient();
            var GetResponse = AltClient.GetObject(BucketName, publictag);
            Assert.Equal(HttpStatusCode.OK, GetResponse.HttpStatusCode);

            var e = Assert.Throws<AggregateException>(() => AltClient.GetObject(BucketName, privatetag));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

            e = Assert.Throws<AggregateException>(() => AltClient.GetObject(BucketName, invalidtag));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
        }

        [Fact(DisplayName = "test_bucket_policy_get_obj_tagging_existing_tag")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Tag Options")]
        [Trait(MainData.Explanation, "[오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 " +
                                     "모든유저에게 GetObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_policy_get_obj_tagging_existing_tag()
        {
            var publictag = "publictag";
            var privatetag = "privatetag";
            var invalidtag = "invalidtag";
            var BucketName = SetupObjects(new List<string>() { publictag, privatetag, invalidtag });
            var Client = GetClient();

            var TagConditional = new JObject() {
                { "StringEquals",
                    new JObject() {
                        { "s3:ExistingObjectTag/security", "public" }
                    }
                }
            };

            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, "*"));
            var PolicyDocument = MakeJsonPolicy("s3:GetObjectTagging", Resource, Conditions: TagConditional);

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="public" },
                    new Tag() { Key= "foo", Value="bar" },
                },
            };
            var Response = Client.PutObjectTagging(BucketName, publictag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="private" },
                },
            };
            Response = Client.PutObjectTagging(BucketName, privatetag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security1", Value="public" },
                },
            };
            Response = Client.PutObjectTagging(BucketName, invalidtag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            var AltClient = GetAltClient();
            var GetResponse = AltClient.GetObjectTagging(BucketName, publictag);
            Assert.Equal(HttpStatusCode.OK, GetResponse.HttpStatusCode);

            var e = Assert.Throws<AggregateException>(() => AltClient.GetObject(BucketName, publictag));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

            e = Assert.Throws<AggregateException>(() => AltClient.GetObjectTagging(BucketName, privatetag));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

            e = Assert.Throws<AggregateException>(() => AltClient.GetObjectTagging(BucketName, invalidtag));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
        }

        [Fact(DisplayName = "test_bucket_policy_put_obj_tagging_existing_tag")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Tag Options")]
        [Trait(MainData.Explanation, "[오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 " +
                                     "모든유저에게 PutObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_policy_put_obj_tagging_existing_tag()
        {
            var publictag = "publictag";
            var privatetag = "privatetag";
            var invalidtag = "invalidtag";
            var BucketName = SetupObjects(new List<string>() { publictag, privatetag, invalidtag });
            var Client = GetClient();

            var TagConditional = new JObject() {
                { "StringEquals",
                    new JObject() {
                        { "s3:ExistingObjectTag/security", "public" }
                    }
                }
            };

            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, "*"));
            var PolicyDocument = MakeJsonPolicy("s3:PutObjectTagging", Resource, Conditions: TagConditional);

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="public" },
                    new Tag() { Key= "foo", Value="bar" },
                },
            };
            var Response = Client.PutObjectTagging(BucketName, publictag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="private" },
                },
            };
            Response = Client.PutObjectTagging(BucketName, privatetag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security1", Value="public" },
                },
            };
            Response = Client.PutObjectTagging(BucketName, invalidtag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);


            var TestTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="public" },
                    new Tag() { Key= "foo", Value="bar" },
                },
            };

            var AltClient = GetAltClient();
            Response = AltClient.PutObjectTagging(BucketName, publictag, TestTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            var e = Assert.Throws<AggregateException>(() => AltClient.PutObjectTagging(BucketName, privatetag, TestTagset));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

            TestTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="private" },
                },
            };
            Response = AltClient.PutObjectTagging(BucketName, publictag, TestTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);


            TestTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="public" },
                    new Tag() { Key= "foo", Value="bar" },
                },
            };

            e = Assert.Throws<AggregateException>(() => AltClient.PutObjectTagging(BucketName, publictag, TestTagset));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
        }

        [Fact(DisplayName = "test_bucket_policy_put_obj_copy_source")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Path Options")]
        [Trait(MainData.Explanation, "[복사하려는 경로명이 'BucketName/public/*'에 해당할 경우에만 모든유저에게 PutObject허용]" +
                                     "조건부 정책설정시 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_policy_put_obj_copy_source()
        {
            var public_foo = "public/foo";
            var public_bar = "public/bar";
            var private_foo = "private/foo";
            var SrcBucketName = SetupObjects(new List<string>() { public_foo, public_bar, private_foo });
            var Client = GetClient();

            var SrcResource = MakeArnResource(string.Format("{0}/{1}", SrcBucketName, "*"));
            var SrcPolicyDocument = MakeJsonPolicy("s3:GetObject", SrcResource);
            Client.PutBucketPolicy(SrcBucketName, SrcPolicyDocument.ToString());

            var DestBucketName = GetNewBucket();

            var TagConditional = new JObject() {
                { "StringLike",
                    new JObject() {
                        { "s3:x-amz-copy-source", string.Format("{0}/public/*", SrcBucketName) }
                    }
                }
            };
            var Resource = MakeArnResource(string.Format("{0}/{1}", DestBucketName, "*"));
            var DestPolicyDocument = MakeJsonPolicy("s3:PutObject", Resource, Conditions: TagConditional);
            Client.PutBucketPolicy(DestBucketName, DestPolicyDocument.ToString());

            var AltClient = GetAltClient();
            var new_foo = "new_foo";
            AltClient.CopyObject(SrcBucketName, public_foo, DestBucketName, new_foo);

            var Response = AltClient.GetObject(DestBucketName, new_foo);
            var Body = GetBody(Response);
            Assert.Equal(public_foo, Body);

            var new_foo2 = "new_foo2";
            AltClient.CopyObject(SrcBucketName, public_bar, DestBucketName, new_foo2);

            var Response2 = AltClient.GetObject(DestBucketName, new_foo2);
            var Body2 = GetBody(Response2);
            Assert.Equal(public_bar, Body2);

            var e = Assert.Throws<AggregateException>(() => AltClient.CopyObject(SrcBucketName, private_foo, DestBucketName, new_foo2));
        }

        [Fact(DisplayName = "test_bucket_policy_put_obj_copy_source_meta")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Metadata Options")]
        [Trait(MainData.Explanation, "[오브젝트의 메타데이터값이'x-amz-metadata-directive=COPY'일 경우에만 모든유저에게 PutObject허용] " +
                                     "조건부 정책설정시 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_policy_put_obj_copy_source_meta()
        {
            var public_foo = "public/foo";
            var public_bar = "public/bar";
            var SrcBucketName = SetupObjects(new List<string>() { public_foo, public_bar });
            var Client = GetClient();

            var SrcResource = MakeArnResource(string.Format("{0}/{1}", SrcBucketName, "*"));
            var PolicyDocument = MakeJsonPolicy("s3:GetObject", SrcResource);
            Client.PutBucketPolicy(SrcBucketName, PolicyDocument.ToString());

            var DestBucketName = GetNewBucket();

            var S3Conditional = new JObject() {
                { "StringEquals",
                    new JObject() {
                        { "s3:x-amz-metadata-directive", "COPY" }
                    }
                }
            };
            var Resource = MakeArnResource(string.Format("{0}/{1}", DestBucketName, "*"));
            PolicyDocument = MakeJsonPolicy("s3:PutObject", Resource, Conditions: S3Conditional);
            Client.PutBucketPolicy(DestBucketName, PolicyDocument.ToString());

            var AltClient = GetAltClient();
            var new_foo = "new_foo";
            AltClient.CopyObject(SrcBucketName, public_foo, DestBucketName, new_foo, MetadataDirective: S3MetadataDirective.COPY);

            var Response = AltClient.GetObject(DestBucketName, new_foo);
            var Body = GetBody(Response);
            Assert.Equal(public_foo, Body);

            var new_foo2 = "new_foo2";
            AltClient.CopyObject(SrcBucketName, public_bar, DestBucketName, new_foo2);

            Response = AltClient.GetObject(DestBucketName, new_foo2);
            Body = GetBody(Response);
            Assert.Equal(public_bar, Body);

            var e = Assert.Throws<AggregateException>(() => AltClient.CopyObject(SrcBucketName, public_bar, DestBucketName, new_foo2, MetadataDirective: S3MetadataDirective.REPLACE));
        }

        [Fact(DisplayName = "test_bucket_policy_put_obj_acl")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "ACL Options")]
        [Trait(MainData.Explanation, "[PutObject는 모든유저에게 허용하지만 권한설정에 'public*'이 포함되면 업로드허용하지 않음] " +
                                     "조건부 정책설정시 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_policy_put_obj_acl()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            var Conditional = new JObject() {
                { "StringLike",
                    new JObject() {
                        { "s3:x-amz-acl", "public*" }
                    }
                }
            };
            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, "*"));
            var s1 = MakeJsonStatement("s3:PutObject", Resource);
            var s2 = MakeJsonStatement("s3:PutObject", Resource, Effect: MainData.PolicyEffectDeny, Conditions: Conditional);
            var PolicyDocument = MakeJsonPolicy(new JArray() { s1, s2 });
            
            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var AltClient = GetAltClient();
            var Key1 = "private-key";

            var Response = AltClient.PutObject(BucketName, Key1, Body: Key1);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            var Key2 = "public-key";
            var Headers = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-acl", "public-read") };

            var e = Assert.Throws<AggregateException>(() => AltClient.PutObject(BucketName, Key2, Body: Key2, HeaderList: Headers));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
        }

        [Fact(DisplayName = "test_bucket_policy_put_obj_grant")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Grant Options")]
        [Trait(MainData.Explanation, "[오브젝트의 grant-full-control이 메인유저일 경우에만 모든유저에게 PutObject허용] " +
                                     "조건부 정책설정시 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_policy_put_obj_grant()
        {
            var BucketName = GetNewBucket();
            var BucketName2 = GetNewBucket();
            var Client = GetClient();

            var MainUserId = Config.MainUser.UserId;
            var AltUserId = Config.AltUser.UserId;

            var OwnerId_str = "id=" + MainUserId;

            var S3Conditional = new JObject() {
                { "StringEquals",
                    new JObject() {
                        { "s3:x-amz-grant-full-control", OwnerId_str}
                    }
                }
            };

            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, "*"));
            var PolicyDocument = MakeJsonPolicy("s3:PutObject", Resource, Conditions: S3Conditional);

            var Resource2 = MakeArnResource(string.Format("{0}/{1}", BucketName2, "*"));
            var PolicyDocument2 = MakeJsonPolicy("s3:PutObject", Resource2);

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());
            Client.PutBucketPolicy(BucketName2, PolicyDocument2.ToString());

            var AltClient = GetAltClient();
            var Key1 = "key1";

            var Headers = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-grant-full-control", OwnerId_str) };

            var Response = AltClient.PutObject(BucketName, Key1, Body: Key1, HeaderList: Headers);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            var Key2 = "key2";
            Response = AltClient.PutObject(BucketName2, Key2, Body: Key2);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            var Acl1Response = Client.GetObjectACL(BucketName, Key1);

            Assert.Throws<AggregateException>(() => Client.GetObjectACL(BucketName2, Key2));

            var Acl2Response = AltClient.GetObjectACL(BucketName2, Key2);

            Assert.Equal(MainUserId, Acl1Response.AccessControlList.Grants[0].Grantee.CanonicalUser);
            Assert.Equal(AltUserId, Acl2Response.AccessControlList.Grants[0].Grantee.CanonicalUser);
        }

        [Fact(DisplayName = "test_bucket_policy_get_obj_acl_existing_tag")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Tag Options")]
        [Trait(MainData.Explanation, "[오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectACL허용]" +
                                     "조건부 정책설정시 올바르게 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_bucket_policy_get_obj_acl_existing_tag()
        {
            var publictag = "publictag";
            var privatetag = "privatetag";
            var invalidtag = "invalidtag";
            var BucketName = SetupObjects(new List<string>() { publictag, privatetag, invalidtag });
            var Client = GetClient();

            var TagConditional = new JObject() {
                { "StringEquals",
                    new JObject() {
                        { "s3:ExistingObjectTag/security", "public" }
                    }
                }
            };

            var Resource = MakeArnResource(string.Format("{0}/{1}", BucketName, "*"));
            var PolicyDocument = MakeJsonPolicy("s3:GetObjectAcl", Resource, Conditions: TagConditional);

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());

            var InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="public" },
                    new Tag() { Key= "foo", Value="bar" },
                },
            };
            var Response = Client.PutObjectTagging(BucketName, publictag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security", Value="private" },
                },
            };
            Response = Client.PutObjectTagging(BucketName, privatetag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            InputTagset = new Tagging()
            {
                TagSet = new List<Tag>()
                {
                    new Tag() { Key= "security1", Value="public" },
                },
            };
            Response = Client.PutObjectTagging(BucketName, invalidtag, InputTagset);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);


            var AltClient = GetAltClient();
            var ACLResponse = AltClient.GetObjectACL(BucketName, publictag);
            Assert.Equal(HttpStatusCode.OK, ACLResponse.HttpStatusCode);

            var e = Assert.Throws<AggregateException>(() => AltClient.GetObject(BucketName, publictag));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

            e = Assert.Throws<AggregateException>(() => AltClient.GetObjectTagging(BucketName, privatetag));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

            e = Assert.Throws<AggregateException>(() => AltClient.GetObjectTagging(BucketName, invalidtag));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
        }

        [Fact(DisplayName = "test_get_publicpolicy_acl_bucket_policy_status")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Status")]
        [Trait(MainData.Explanation, "[모든 사용자가 버킷에 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_get_publicpolicy_acl_bucket_policy_status()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            Assert.Throws<AggregateException>(() => Client.GetBucketPolicyStatus(BucketName));

            var Resource1 = MainData.PolicyResourcePrefix + BucketName;
            var Resource2 = MainData.PolicyResourcePrefix + BucketName + "/*";

            var PolicyDocument = new JObject()
            {
                {MainData.PolicyVersion, MainData.PolicyVersionDate },
                { MainData.PolicyStatement, new JArray()
                {
                     new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectAllow },
                                    { MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
                                    { MainData.PolicyAction, "s3:ListBucket" },
                                    { MainData.PolicyResource, new JArray() { Resource1, Resource2 } },
                     },
                    }
                },
            };

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());
            var Response = Client.GetBucketPolicyStatus(BucketName);
            Assert.True(Response.PolicyStatus.IsPublic);
        }

        [Fact(DisplayName = "test_get_nonpublicpolicy_acl_bucket_policy_status")]
        [Trait(MainData.Major, "Policy")]
        [Trait(MainData.Minor, "Status")]
        [Trait(MainData.Explanation, "[특정 ip로 접근했을때만 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_get_nonpublicpolicy_acl_bucket_policy_status()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            Assert.Throws<AggregateException>(() => Client.GetBucketPolicyStatus(BucketName));

            var Resource1 = MainData.PolicyResourcePrefix + BucketName;
            var Resource2 = MainData.PolicyResourcePrefix + BucketName + "/*";

            var PolicyDocument = new JObject()
            {
                {MainData.PolicyVersion, MainData.PolicyVersionDate },
                { MainData.PolicyStatement, new JArray()
                {
                     new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectAllow },
                                    { MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
                                    { MainData.PolicyAction, "s3:ListBucket" },
                                    { MainData.PolicyResource, new JArray() { Resource1, Resource2 } },
                                    { MainData.PolicyCondition, new JObject() { { "IpAddress", new JObject() { { "aws:SourceIp", "10.0.0.0/32" } } } } }
                     },
                    }
                },
            };

            Client.PutBucketPolicy(BucketName, PolicyDocument.ToString());
            var Response = Client.GetBucketPolicyStatus(BucketName);
            Assert.False(Response.PolicyStatus.IsPublic);
        }
    }
}
