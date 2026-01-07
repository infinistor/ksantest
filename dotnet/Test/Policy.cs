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
using Amazon.S3.Model;
using Newtonsoft.Json.Linq;
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Policy : TestBase
	{
		public Policy(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷에 정책 설정이 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicy()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "asdf";
			client.PutObject(bucketName, Key, body: Key);

			var Resource1 = "arn:aws:s3:::" + bucketName;
			var Resource2 = "arn:aws:s3:::" + bucketName + "/*";

			var PolicyDocument = new JObject()
			{
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
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

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var AltClient = GetAltClient();
			var Response = AltClient.ListObjects(bucketName);
			Assert.Single(Response.S3Objects);

			var GetResponse = client.GetBucketPolicy(bucketName);
			Assert.Equal(PolicyDocument.ToString().Replace("\r\n", "").Replace(" ", ""), GetResponse.Policy.Replace("\r\n", "").Replace(" ", ""));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷에 정책 설정이 올바르게 적용되는지 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucketv2_policy()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "asdf";
			client.PutObject(bucketName, Key, body: Key);

			var Resource1 = "arn:aws:s3:::" + bucketName;
			var Resource2 = "arn:aws:s3:::" + bucketName + "/*";

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

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var AltClient = GetAltClient();
			var Response = AltClient.ListObjectsV2(bucketName);
			Assert.Single(Response.S3Objects);

			var GetResponse = client.GetBucketPolicy(bucketName);
			Assert.Equal(PolicyDocument.ToString().Replace("\r\n", "").Replace(" ", ""), GetResponse.Policy.Replace("\r\n", "").Replace(" ", ""));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Priority")]
		[Trait(MainData.Explanation, "버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_policy_acl()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "asdf";
			client.PutObject(bucketName, Key, body: Key);

			var Resource1 = "arn:aws:s3:::" + bucketName;
			var Resource2 = "arn:aws:s3:::" + bucketName + "/*";

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
			client.PutBucketACL(bucketName, acl: S3CannedACL.AuthenticatedRead);
			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var AltClient = GetAltClient();
			var e = Assert.Throws<AggregateException>(() => AltClient.ListObjects(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			client.DeleteBucketPolicy(bucketName);
			client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Priority")]
		[Trait(MainData.Explanation, "버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucketv2_policy_acl()
		{

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "asdf";
			client.PutObject(bucketName, Key, body: Key);

			var Resource1 = "arn:aws:s3:::" + bucketName;
			var Resource2 = "arn:aws:s3:::" + bucketName + "/*";

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
			client.PutBucketACL(bucketName, acl: S3CannedACL.AuthenticatedRead);
			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var AltClient = GetAltClient();
			var e = Assert.Throws<AggregateException>(() => AltClient.ListObjectsV2(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			client.DeleteBucketPolicy(bucketName);
			client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Taggings")]
		[Trait(MainData.Explanation, "정책설정으로 오브젝트의 태그목록 읽기를 public-read로 설정했을때 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_get_tags_acl_public()
		{
			var Key = "testgettagsacl";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, Key));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:GetObjectTagging", Resource);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var InputTagset = S3Utils.MakeSimpleTagset(10);
			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagset);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var AltClient = GetAltClient();

			var GetResponse = AltClient.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagset.TagSet, GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Tagging")]
		[Trait(MainData.Explanation, "정책설정으로 오브젝트의 태그 입력을 public-read로 설정했을때 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_put_tags_acl_public()
		{
			var Key = "testputtagsacl";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, Key));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:PutObjectTagging", Resource);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var InputTagset = S3Utils.MakeSimpleTagset(10);
			var AltClient = GetAltClient();
			var PutResponse = AltClient.PutObjectTagging(bucketName, Key, InputTagset);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagset.TagSet, GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Tagging")]
		[Trait(MainData.Explanation, "정책설정으로 오브젝트의 태그 삭제를 public-read로 설정했을때 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_delete_tags_obj_public()
		{
			var Key = "testdeltagsacl";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, Key));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:DeleteObjectTagging", Resource);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var InputTagset = S3Utils.MakeSimpleTagset(10);
			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagset);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var AltClient = GetAltClient();
			var DelResponse = AltClient.DeleteObjectTagging(bucketName, Key);
			Assert.Equal(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			Assert.Empty(GetResponse.Tagging);
		}

		[Fact]
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
			var bucketName = SetupObjects([publictag, privatetag, invalidtag]);
			var client = GetClient();

			var TagConditional = new JObject() {
				{ "StringEquals",
					new JObject() {
						{ "s3:ExistingObjectTag/security", "public" }
					}
				}
			};

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, "*"));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:GetObject", Resource, conditions: TagConditional);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="public" },
					new() { Key= "foo", Value="bar" },
				],
			};
			var Response = client.PutObjectTagging(bucketName, publictag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="private" },
				],
			};
			Response = client.PutObjectTagging(bucketName, privatetag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security1", Value="public" },
				],
			};
			Response = client.PutObjectTagging(bucketName, invalidtag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var AltClient = GetAltClient();
			var GetResponse = AltClient.GetObject(bucketName, publictag);
			Assert.Equal(HttpStatusCode.OK, GetResponse.HttpStatusCode);

			var e = Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, privatetag));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, invalidtag));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
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
			var bucketName = SetupObjects([publictag, privatetag, invalidtag]);
			var client = GetClient();

			var TagConditional = new JObject() {
				{ "StringEquals",
					new JObject() {
						{ "s3:ExistingObjectTag/security", "public" }
					}
				}
			};

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, "*"));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:GetObjectTagging", Resource, conditions: TagConditional);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="public" },
					new() { Key= "foo", Value="bar" },
				],
			};
			var Response = client.PutObjectTagging(bucketName, publictag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="private" },
				],
			};
			Response = client.PutObjectTagging(bucketName, privatetag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security1", Value="public" },
				],
			};
			Response = client.PutObjectTagging(bucketName, invalidtag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var AltClient = GetAltClient();
			var GetResponse = AltClient.GetObjectTagging(bucketName, publictag);
			Assert.Equal(HttpStatusCode.OK, GetResponse.HttpStatusCode);

			var e = Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, publictag));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => AltClient.GetObjectTagging(bucketName, privatetag));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => AltClient.GetObjectTagging(bucketName, invalidtag));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
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
			var bucketName = SetupObjects([publictag, privatetag, invalidtag]);
			var client = GetClient();

			var TagConditional = new JObject() {
				{ "StringEquals",
					new JObject() {
						{ "s3:ExistingObjectTag/security", "public" }
					}
				}
			};

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, "*"));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:PutObjectTagging", Resource, conditions: TagConditional);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="public" },
					new() { Key= "foo", Value="bar" },
				],
			};
			var Response = client.PutObjectTagging(bucketName, publictag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="private" },
				],
			};
			Response = client.PutObjectTagging(bucketName, privatetag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security1", Value="public" },
				],
			};
			Response = client.PutObjectTagging(bucketName, invalidtag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);


			var TestTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="public" },
					new() { Key= "foo", Value="bar" },
				],
			};

			var AltClient = GetAltClient();
			Response = AltClient.PutObjectTagging(bucketName, publictag, TestTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var e = Assert.Throws<AggregateException>(() => AltClient.PutObjectTagging(bucketName, privatetag, TestTagset));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			TestTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="private" },
				],
			};
			Response = AltClient.PutObjectTagging(bucketName, publictag, TestTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);


			TestTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="public" },
					new() { Key= "foo", Value="bar" },
				],
			};

			e = Assert.Throws<AggregateException>(() => AltClient.PutObjectTagging(bucketName, publictag, TestTagset));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Path Options")]
		[Trait(MainData.Explanation, "[복사하려는 경로명이 'bucketName/public/*'에 해당할 경우에만 모든유저에게 PutObject허용]" +
									 "조건부 정책설정시 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_policy_put_obj_copy_source()
		{
			var public_foo = "public/foo";
			var public_bar = "public/bar";
			var private_foo = "private/foo";
			var SrcbucketName = SetupObjects([public_foo, public_bar, private_foo]);
			var client = GetClient();

			var SrcResource = S3Utils.MakeArnResource(string.Format("{0}/{1}", SrcbucketName, "*"));
			var SrcPolicyDocument = S3Utils.MakeJsonPolicy("s3:GetObject", SrcResource);
			client.PutBucketPolicy(SrcbucketName, SrcPolicyDocument.ToString());

			var DestbucketName = GetNewBucket();

			var TagConditional = new JObject() {
				{ "StringLike",
					new JObject() {
						{ "s3:x-amz-copy-source", string.Format("{0}/public/*", SrcbucketName) }
					}
				}
			};
			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", DestbucketName, "*"));
			var DestPolicyDocument = S3Utils.MakeJsonPolicy("s3:PutObject", Resource, conditions: TagConditional);
			client.PutBucketPolicy(DestbucketName, DestPolicyDocument.ToString());

			var AltClient = GetAltClient();
			var new_foo = "new_foo";
			AltClient.CopyObject(SrcbucketName, public_foo, DestbucketName, new_foo);

			var Response = AltClient.GetObject(DestbucketName, new_foo);
			var body = S3Utils.GetBody(Response);
			Assert.Equal(public_foo, body);

			var new_foo2 = "new_foo2";
			AltClient.CopyObject(SrcbucketName, public_bar, DestbucketName, new_foo2);

			var Response2 = AltClient.GetObject(DestbucketName, new_foo2);
			var Body2 = S3Utils.GetBody(Response2);
			Assert.Equal(public_bar, Body2);

			var e = Assert.Throws<AggregateException>(() => AltClient.CopyObject(SrcbucketName, private_foo, DestbucketName, new_foo2));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Metadata Options")]
		[Trait(MainData.Explanation, "[오브젝트의 메타데이터값이'x-amz-metadata-directive=COPY'일 경우에만 모든유저에게 PutObject허용] " +
									 "조건부 정책설정시 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_policy_put_obj_copy_source_meta()
		{
			var public_foo = "public/foo";
			var public_bar = "public/bar";
			var SrcbucketName = SetupObjects([public_foo, public_bar]);
			var client = GetClient();

			var SrcResource = S3Utils.MakeArnResource(string.Format("{0}/{1}", SrcbucketName, "*"));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:GetObject", SrcResource);
			client.PutBucketPolicy(SrcbucketName, PolicyDocument.ToString());

			var DestbucketName = GetNewBucket();

			var S3Conditional = new JObject() {
				{ "StringEquals",
					new JObject() {
						{ "s3:x-amz-metadata-directive", "COPY" }
					}
				}
			};
			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", DestbucketName, "*"));
			PolicyDocument = S3Utils.MakeJsonPolicy("s3:PutObject", Resource, conditions: S3Conditional);
			client.PutBucketPolicy(DestbucketName, PolicyDocument.ToString());

			var AltClient = GetAltClient();
			var new_foo = "new_foo";
			AltClient.CopyObject(SrcbucketName, public_foo, DestbucketName, new_foo, metadataDirective: S3MetadataDirective.COPY);

			var Response = AltClient.GetObject(DestbucketName, new_foo);
			var body = S3Utils.GetBody(Response);
			Assert.Equal(public_foo, body);

			var new_foo2 = "new_foo2";
			AltClient.CopyObject(SrcbucketName, public_bar, DestbucketName, new_foo2);

			Response = AltClient.GetObject(DestbucketName, new_foo2);
			body = S3Utils.GetBody(Response);
			Assert.Equal(public_bar, body);

			var e = Assert.Throws<AggregateException>(() => AltClient.CopyObject(SrcbucketName, public_bar, DestbucketName, new_foo2, metadataDirective: S3MetadataDirective.REPLACE));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "acl Options")]
		[Trait(MainData.Explanation, "[PutObject는 모든유저에게 허용하지만 권한설정에 'public*'이 포함되면 업로드허용하지 않음] " +
									 "조건부 정책설정시 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_policy_put_obj_acl()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Conditional = new JObject() {
				{ "StringLike",
					new JObject() {
						{ "s3:x-amz-acl", "public*" }
					}
				}
			};
			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, "*"));
			var s1 = S3Utils.MakeJsonStatement("s3:PutObject", Resource);
			var s2 = S3Utils.MakeJsonStatement("s3:PutObject", Resource, effect: MainData.PolicyEffectDeny, conditions: Conditional);
			var PolicyDocument = S3Utils.MakeJsonPolicy([s1, s2]);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var AltClient = GetAltClient();
			var Key1 = "private-key";

			var Response = AltClient.PutObject(bucketName, Key1, body: Key1);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var Key2 = "public-key";
			var Headers = new List<KeyValuePair<string, string>>() { new("x-amz-acl", "public-read") };

			var e = Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key2, body: Key2, headerList: Headers));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Grant Options")]
		[Trait(MainData.Explanation, "[오브젝트의 grant-full-control이 메인유저일 경우에만 모든유저에게 PutObject허용] " +
									 "조건부 정책설정시 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_policy_put_obj_grant()
		{
			var bucketName = GetNewBucket();
			var bucketName2 = GetNewBucket();
			var client = GetClient();

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

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, "*"));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:PutObject", Resource, conditions: S3Conditional);

			var Resource2 = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName2, "*"));
			var PolicyDocument2 = S3Utils.MakeJsonPolicy("s3:PutObject", Resource2);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());
			client.PutBucketPolicy(bucketName2, PolicyDocument2.ToString());

			var AltClient = GetAltClient();
			var Key1 = "key1";

			var Headers = new List<KeyValuePair<string, string>>() { new("x-amz-grant-full-control", OwnerId_str) };

			var Response = AltClient.PutObject(bucketName, Key1, body: Key1, headerList: Headers);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var Key2 = "key2";
			Response = AltClient.PutObject(bucketName2, Key2, body: Key2);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var Acl1Response = client.GetObjectACL(bucketName, Key1);

			Assert.Throws<AggregateException>(() => client.GetObjectACL(bucketName2, Key2));

			var Acl2Response = AltClient.GetObjectACL(bucketName2, Key2);

			Assert.Equal(MainUserId, Acl1Response.AccessControlList.Grants[0].Grantee.CanonicalUser);
			Assert.Equal(AltUserId, Acl2Response.AccessControlList.Grants[0].Grantee.CanonicalUser);
		}

		[Fact]
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
			var bucketName = SetupObjects([publictag, privatetag, invalidtag]);
			var client = GetClient();

			var TagConditional = new JObject() {
				{ "StringEquals",
					new JObject() {
						{ "s3:ExistingObjectTag/security", "public" }
					}
				}
			};

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/{1}", bucketName, "*"));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:GetObjectAcl", Resource, conditions: TagConditional);

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());

			var InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="public" },
					new() { Key= "foo", Value="bar" },
				],
			};
			var Response = client.PutObjectTagging(bucketName, publictag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security", Value="private" },
				],
			};
			Response = client.PutObjectTagging(bucketName, privatetag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			InputTagset = new Tagging()
			{
				TagSet =
				[
					new() { Key= "security1", Value="public" },
				],
			};
			Response = client.PutObjectTagging(bucketName, invalidtag, InputTagset);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);


			var AltClient = GetAltClient();
			var ACLResponse = AltClient.GetObjectACL(bucketName, publictag);
			Assert.Equal(HttpStatusCode.OK, ACLResponse.HttpStatusCode);

			var e = Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, publictag));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => AltClient.GetObjectTagging(bucketName, privatetag));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => AltClient.GetObjectTagging(bucketName, invalidtag));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Status")]
		[Trait(MainData.Explanation, "[모든 사용자가 버킷에 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_get_publicpolicy_acl_bucket_policy_status()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			Assert.Throws<AggregateException>(() => client.GetBucketPolicyStatus(bucketName));

			var Resource1 = MainData.PolicyResourcePrefix + bucketName;
			var Resource2 = MainData.PolicyResourcePrefix + bucketName + "/*";

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

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());
			var Response = client.GetBucketPolicyStatus(bucketName);
			Assert.True(Response.PolicyStatus.IsPublic);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Status")]
		[Trait(MainData.Explanation, "[특정 ip로 접근했을때만 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetNonpublicpolicyAclBucketPolicyStatus()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			Assert.Throws<AggregateException>(() => client.GetBucketPolicyStatus(bucketName));

			var Resource1 = MainData.PolicyResourcePrefix + bucketName;
			var Resource2 = MainData.PolicyResourcePrefix + bucketName + "/*";

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

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());
			var Response = client.GetBucketPolicyStatus(bucketName);
			Assert.False(Response.PolicyStatus.IsPublic);
		}
	}
}
