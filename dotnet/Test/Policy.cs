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
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var Key = "TestBucketPolicy";
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
		public void TestBucketV2Policy()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var Key = "TestBucketV2Policy";
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
		public void TestBucketPolicyAcl()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var Key = "TestBucketPolicyAcl";
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
		public void TestBucketV2PolicyAcl()
		{
			TestId = 4;

			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var Key = "TestBucketV2PolicyAcl";
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
		public void TestGetTagsAclPublic()
		{
			TestId = 5;
			var Key = "TestGetTagsAclPublic";
			var client = GetClient();
			var bucketName = SetupKeyWithRandomContent(Key, bucketName: GetNewBucketCannedAcl(client), client: client);

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
		public void TestPutTagsAclPublic()
		{
			TestId = 6;
			var Key = "TestPutTagsAclPublic";
			var client = GetClient();
			var bucketName = SetupKeyWithRandomContent(Key, bucketName: GetNewBucketCannedAcl(client), client: client);

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
		public void TestDeleteTagsObjPublic()
		{
			TestId = 7;
			var Key = "TestDeleteTagsObjPublic";
			var client = GetClient();
			var bucketName = SetupKeyWithRandomContent(Key, bucketName: GetNewBucketCannedAcl(client), client: client);

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
		public void TestBucketPolicyGetObjExistingTag()
		{
			TestId = 8;
			var publictag = "publictag";
			var privatetag = "privatetag";
			var invalidtag = "invalidtag";
			var client = GetClient();
			var bucketName = SetupObjects([publictag, privatetag, invalidtag], GetNewBucketCannedAcl(client));

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
		public void TestBucketPolicyGetObjTaggingExistingTag()
		{
			TestId = 9;
			var publictag = "publictag";
			var privatetag = "privatetag";
			var invalidtag = "invalidtag";
			var client = GetClient();
			var bucketName = SetupObjects([publictag, privatetag, invalidtag], GetNewBucketCannedAcl(client));

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
		public void TestBucketPolicyPutObjTaggingExistingTag()
		{
			TestId = 10;
			var publictag = "publictag";
			var privatetag = "privatetag";
			var invalidtag = "invalidtag";
			var client = GetClient();
			var bucketName = SetupObjects([publictag, privatetag, invalidtag], GetNewBucketCannedAcl(client));

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
		public void TestBucketPolicyPutObjCopySource()
		{
			TestId = 11;
			var public_foo = "public/foo";
			var public_bar = "public/bar";
			var private_foo = "private/foo";
			var client = GetClient();
			var SrcbucketName = SetupObjects([public_foo, public_bar, private_foo], GetNewBucketCannedAcl(client));

			var SrcResource = S3Utils.MakeArnResource(string.Format("{0}/{1}", SrcbucketName, "*"));
			var SrcPolicyDocument = S3Utils.MakeJsonPolicy("s3:GetObject", SrcResource);
			client.PutBucketPolicy(SrcbucketName, SrcPolicyDocument.ToString());

			var DestbucketName = GetNewBucketCannedAcl(client);

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
		public void TestBucketPolicyPutObjCopySourceMeta()
		{
			TestId = 12;
			var public_foo = "public/foo";
			var public_bar = "public/bar";
			var client = GetClient();
			var SrcbucketName = SetupObjects([public_foo, public_bar], GetNewBucketCannedAcl(client));

			var SrcResource = S3Utils.MakeArnResource(string.Format("{0}/{1}", SrcbucketName, "*"));
			var PolicyDocument = S3Utils.MakeJsonPolicy("s3:GetObject", SrcResource);
			client.PutBucketPolicy(SrcbucketName, PolicyDocument.ToString());

			var DestbucketName = GetNewBucketCannedAcl(client);

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
		public void TestBucketPolicyPutObjAcl()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

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
			var Key1 = "TestBucketPolicyPutObjAcl-000";

			var Response = AltClient.PutObject(bucketName, Key1, body: Key1);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var Key2 = "TestBucketPolicyPutObjAcl-001";
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
		public void TestBucketPolicyPutObjGrant()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var bucketName2 = GetNewBucketCannedAcl(client);

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
			var Key1 = "TestBucketPolicyPutObjGrant-000";

			var Headers = new List<KeyValuePair<string, string>>() { new("x-amz-grant-full-control", OwnerId_str) };

			var Response = AltClient.PutObject(bucketName, Key1, body: Key1, headerList: Headers);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var Key2 = "TestBucketPolicyPutObjGrant-001";
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
		public void TestBucketPolicyGetObjAclExistingTag()
		{
			TestId = 15;
			var publictag = "publictag";
			var privatetag = "privatetag";
			var invalidtag = "invalidtag";
			var client = GetClient();
			var bucketName = SetupObjects([publictag, privatetag, invalidtag], GetNewBucketCannedAcl(client));

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
			Assert.NotNull(ACLResponse);

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
		public void TestBucketPolicyStatusWithAllUser()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

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
		[Trait(MainData.Explanation, "[특정 사용자(CanonicalUser)에게만 접근을 허용하는 정책] 버킷의 정책상태가 public이 아님을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyStatusWithSpecificUserAccess()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			Assert.Throws<AggregateException>(() => client.GetBucketPolicyStatus(bucketName));

			var Resource1 = MainData.PolicyResourcePrefix + bucketName;
			var Resource2 = MainData.PolicyResourcePrefix + bucketName + "/*";

			var PolicyDocument = new JObject()
			{
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, new JArray()
				{
					 new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectAllow },
									{ MainData.PolicyPrincipal, new JObject() { { "CanonicalUser", Config.MainUser.UserId } } },
									{ MainData.PolicyAction, "s3:ListBucket" },
									{ MainData.PolicyResource, new JArray() { Resource1, Resource2 } },
					 },
					}
				},
			};

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());
			var Response = client.GetBucketPolicyStatus(bucketName);
			Assert.False(Response.PolicyStatus.IsPublic);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Status")]
		[Trait(MainData.Explanation, "[매우 넓은 IP 범위(0.0.0.0/1)를 허용하는 정책] 버킷의 정책상태가 public임을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyStatusWithWideIPRange()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			var PolicyDocument = new JObject()
			{
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, new JArray()
				{
					 new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectAllow },
									{ MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
									{ MainData.PolicyAction, "s3:GetObject" },
									{ MainData.PolicyResource, S3Utils.MakeArnResource(string.Format("{0}/*", bucketName)) },
									{ MainData.PolicyCondition, new JObject() { { "IpAddress", new JObject() { { "aws:SourceIp", "0.0.0.0/1" } } } } },
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
		[Trait(MainData.Explanation, "[특정 IP 범위(192.168.1.0/24)만 허용하는 정책] 버킷의 정책상태가 public이 아님을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyStatusWithIPRange()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			var PolicyDocument = new JObject()
			{
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, new JArray()
				{
					 new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectAllow },
									{ MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
									{ MainData.PolicyAction, "s3:GetObject" },
									{ MainData.PolicyResource, new JArray() { S3Utils.MakeArnResource(string.Format("{0}/*", bucketName)) } },
									{ MainData.PolicyCondition, new JObject() { { "IpAddress", new JObject() { { "aws:SourceIp", "192.168.1.0/24" } } } } },
					 },
					}
				},
			};

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());
			var Response = client.GetBucketPolicyStatus(bucketName);
			Assert.False(Response.PolicyStatus.IsPublic);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Status")]
		[Trait(MainData.Explanation, "[특정 시간대에만 접근을 허용하는 정책] 버킷의 정책상태가 public임을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyStatusWithTimeCondition()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/*", bucketName));

			var StartTime = DateTime.UtcNow.AddMinutes(10).ToString("yyyy-MM-ddTHH:mm:ssZ");
			var EndTime = DateTime.UtcNow.AddMinutes(10).AddSeconds(1).ToString("yyyy-MM-ddTHH:mm:ssZ");

			var PolicyDocument = new JObject()
			{
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, new JArray()
				{
					 new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectAllow },
									{ MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
									{ MainData.PolicyAction, "s3:GetObject" },
									{ MainData.PolicyResource, Resource },
									{ MainData.PolicyCondition, new JObject() {
										{ "DateGreaterThan", new JObject() { { "aws:CurrentTime", StartTime } } },
										{ "DateLessThan", new JObject() { { "aws:CurrentTime", EndTime } } },
									} },
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
		[Trait(MainData.Explanation, "[오브젝트 태그 조건이 있는 정책] 버킷의 정책상태가 public임을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyStatusWithTagCondition()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			var Resource = S3Utils.MakeArnResource(string.Format("{0}/*", bucketName));

			var PolicyDocument = new JObject()
			{
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, new JArray()
				{
					 new JObject(){ { MainData.PolicyEffect, MainData.PolicyEffectAllow },
									{ MainData.PolicyPrincipal, new JObject() { { "AWS", "*" } } },
									{ MainData.PolicyAction, "s3:GetObject" },
									{ MainData.PolicyResource, Resource },
									{ MainData.PolicyCondition, new JObject() { { "StringEquals", new JObject() { { "s3:ExistingObjectTag/access", "restricted" } } } } },
					 },
					}
				},
			};

			client.PutBucketPolicy(bucketName, PolicyDocument.ToString());
			var Response = client.GetBucketPolicyStatus(bucketName);
			Assert.True(Response.PolicyStatus.IsPublic);
		}

		/// <summary>모든 주체의 GetObject 를 거부하는 문장. (python _deny_statement)</summary>
		private static JObject DenyStatement(string bucketName) => new()
		{
			{ MainData.PolicyEffect, MainData.PolicyEffectDeny },
			{ MainData.PolicyPrincipal, "*" },
			{ MainData.PolicyAction, "s3:GetObject" },
			{ MainData.PolicyResource, $"{MainData.PolicyResourcePrefix}{bucketName}/*" },
		};

		/// <summary>거부 문장의 한 항목을 덮어쓴다.</summary>
		private static JObject DenyStatement(string bucketName, string name, JToken value)
		{
			var statement = DenyStatement(bucketName);
			statement[name] = value;
			return statement;
		}

		private static string MakePolicyDocument(string version, params JObject[] statements) => new JObject()
		{
			{ MainData.PolicyVersion, version },
			{ MainData.PolicyStatement, new JArray(statements) },
		}.ToString(Newtonsoft.Json.Formatting.None);

		private static string MakePolicyDocument(params JObject[] statements)
			=> MakePolicyDocument(MainData.PolicyVersionDate, statements);

		private static void AssertMalformedPolicy(S3Client client, string bucketName, string policy)
			=> AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_POLICY, () => client.PutBucketPolicy(bucketName, policy));

		private static void AssertSamePolicy(string expected, string actual)
			=> Assert.True(JToken.DeepEquals(JObject.Parse(expected), JObject.Parse(actual)), $"expected: {expected}, actual: {actual}");

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "정책이 없는 버킷의 정책·정책 상태를 조회하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketPolicyGetNotExist()
		{
			TestId = 22;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketPolicy(bucketName), MainData.NO_SUCH_BUCKET_POLICY);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET_POLICY, () => client.GetBucketPolicyStatus(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "정책이 없는 버킷의 정책을 삭제해도 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyDeleteNotExist()
		{
			TestId = 23;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 정책이 없어도 삭제는 성공한다
			var response = client.DeleteBucketPolicy(bucketName);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷 정책의 설정, 조회, 삭제가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyPutGetDelete()
		{
			TestId = 24;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var policy = MakePolicyDocument(DenyStatement(bucketName, "Sid", "deny-get"));
			client.PutBucketPolicy(bucketName, policy);

			AssertSamePolicy(policy, client.GetBucketPolicy(bucketName).Policy);
			Assert.False(client.GetBucketPolicyStatus(bucketName).PolicyStatus.IsPublic);

			client.DeleteBucketPolicy(bucketName);
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketPolicy(bucketName), MainData.NO_SUCH_BUCKET_POLICY);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 정책을 조회, 설정, 삭제하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketPolicyBucketNotExist()
		{
			TestId = 25;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			var policy = MakePolicyDocument(DenyStatement(bucketName));
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketPolicy(bucketName), MainData.NO_SUCH_BUCKET);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.PutBucketPolicy(bucketName, policy));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.DeleteBucketPolicy(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "다른 계정이 버킷 정책을 조회, 설정, 삭제하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketPolicyAccessDenied()
		{
			TestId = 26;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			var policy = MakePolicyDocument(DenyStatement(bucketName));
			client.PutBucketPolicy(bucketName, policy);

			AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, () => altClient.GetBucketPolicy(bucketName));
			AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, () => altClient.PutBucketPolicy(bucketName, policy));
			AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, () => altClient.DeleteBucketPolicy(bucketName));
			// 정책은 그대로 남아 있다
			AssertSamePolicy(policy, client.GetBucketPolicy(bucketName).Policy);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "JSON 형식이 아니거나 문장이 없거나 버전이 잘못된 정책을 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketPolicyInvalidJson()
		{
			TestId = 27;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertMalformedPolicy(client, bucketName, "not json");
			// Statement 가 비어 있다
			AssertMalformedPolicy(client, bucketName, MakePolicyDocument());
			AssertMalformedPolicy(client, bucketName, MakePolicyDocument("2020-01-01", DenyStatement(bucketName)));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "정책 문장의 항목 값이 잘못되면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketPolicyInvalidElement()
		{
			TestId = 28;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var invalidStatements = new List<JObject>()
			{
				DenyStatement(bucketName, MainData.PolicyEffect, "Maybe"),
				DenyStatement(bucketName, MainData.PolicyAction, "s3:Invalid"),
				DenyStatement(bucketName, MainData.PolicyPrincipal, new JObject() { { "AWS", "arn:aws:iam::000000000000:user/not-exist" } }),
				// 없는 조건 키
				DenyStatement(bucketName, MainData.PolicyCondition, new JObject() { { "StringEquals", new JObject() { { "s3:invalid", "value" } } } }),
				// 없는 조건 연산자
				DenyStatement(bucketName, MainData.PolicyCondition, new JObject() { { "InvalidEquals", new JObject() { { "aws:SourceIp", "1.1.1.1" } } } }),
			};
			foreach (var statement in invalidStatements)
				AssertMalformedPolicy(client, bucketName, MakePolicyDocument(statement));
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketPolicy(bucketName), MainData.NO_SUCH_BUCKET_POLICY);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "정책 문장의 리소스가 잘못되거나 필수 항목이 없으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketPolicyInvalidResource()
		{
			TestId = 29;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 다른 버킷의 리소스
			AssertMalformedPolicy(client, bucketName,
				MakePolicyDocument(DenyStatement(bucketName, MainData.PolicyResource, "arn:aws:s3:::other-bucket-name/*")));
			// 버킷 대상 액션에 오브젝트 리소스
			AssertMalformedPolicy(client, bucketName,
				MakePolicyDocument(DenyStatement(bucketName, MainData.PolicyAction, "s3:ListBucket")));
			// Resource, Principal 누락
			foreach (var missing in new[] { MainData.PolicyResource, MainData.PolicyPrincipal })
			{
				var statement = DenyStatement(bucketName);
				statement.Remove(missing);
				AssertMalformedPolicy(client, bucketName, MakePolicyDocument(statement));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Max")]
		[Trait(MainData.Explanation, "20KB를 넘는 정책을 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketPolicyTooLarge()
		{
			TestId = 30;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 정책 문서는 20KB 까지
			var statements = new JObject[70];
			for (int i = 0; i < statements.Length; i++)
			{
				statements[i] = DenyStatement(bucketName, "Sid", $"statement{i}");
				statements[i][MainData.PolicyCondition] = new JObject() { { "StringEquals", new JObject() { { "aws:Referer", new string('x', 180) } } } };
			}
			var policy = MakePolicyDocument(statements);
			Assert.True(policy.Length > 20480);
			AssertMalformedPolicy(client, bucketName, policy);
		}
	}
}
