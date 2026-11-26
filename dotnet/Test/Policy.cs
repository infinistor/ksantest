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

		#region 정책 효과 (AWS 실측 기준)
		// 정책을 바꾼 뒤에는 새 클라이언트로 확인한다(같은 연결은 바뀌기 전 권한으로 답할 수 있다).
		// Deny 는 정리에 필요 없는 액션(GetObject, PutObject 등)으로만 건다.

		private JObject AltPrincipal() => new() { { "CanonicalUser", Config.AltUser.UserId } };

		/// <summary>문장을 만든다. action, resource 가 null 이면 그 항목을 뺀다(NotAction 등으로 바꿀 때).</summary>
		private JObject Statement(string effect, JToken action, JToken resource, JToken principal = null, JObject condition = null)
		{
			var statement = new JObject()
			{
				{ MainData.PolicyEffect, effect },
				{ MainData.PolicyPrincipal, principal ?? AltPrincipal() },
			};
			if (action != null) statement[MainData.PolicyAction] = action;
			if (resource != null) statement[MainData.PolicyResource] = resource;
			if (condition != null) statement[MainData.PolicyCondition] = condition;
			return statement;
		}

		/// <summary>문장의 한 항목을 덮어쓴다. 값이 null 이면 그 항목을 뺀다.</summary>
		private static JObject With(JObject statement, string name, JToken value)
		{
			if (value == null) statement.Remove(name);
			else statement[name] = value;
			return statement;
		}

		private static JObject Condition(string op, string name, JToken value) => new() { { op, new JObject() { { name, value } } } };

		private static string ObjectArn(string bucketName, string path = "*") => S3Utils.MakeArnResource($"{bucketName}/{path}");

		private static JArray BucketAndObjectArn(string bucketName) => [S3Utils.MakeArnResource(bucketName), ObjectArn(bucketName)];

		private static void PutPolicy(S3Client client, string bucketName, params JObject[] statements)
			=> client.PutBucketPolicy(bucketName, MakePolicyDocument(statements));

		private static void AssertDenied(Action call) => AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, call);

		// SDK v4 는 빈 목록을 null 로 돌려줄 수 있다
		private static void AssertEmptyList<T>(List<T> list) => Assert.True(list == null || list.Count == 0);

		/// <summary>.NET SDK는 키별 오류가 있으면 DeleteObjectsException을 던지므로 예외에서 응답을 꺼낸다.</summary>
		private static DeleteObjectsResponse DeleteObjectsResponseOf(S3Client client, string bucketName, List<KeyVersion> keys)
		{
			try { return client.DeleteObjects(bucketName, keys); }
			catch (AggregateException e) when (e.InnerException is DeleteObjectsException de) { return de.Response; }
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Effect")]
		[Trait(MainData.Explanation, "특정 사용자에게 GetObject 를 허용하면 그 액션만 허용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyAllowUser()
		{
			TestId = 31;
			var key = "TestBucketPolicyAllowUser";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			PutPolicy(client, bucketName, Statement("Allow", "s3:GetObject", ObjectArn(bucketName)));
			Assert.False(client.GetBucketPolicyStatus(bucketName).PolicyStatus.IsPublic);

			var altClient = GetAltClient();
			Assert.Equal("content", GetBody(altClient.GetObject(bucketName, key)));
			Assert.Equal(7, altClient.HeadObject(bucketName, key).ContentLength);
			// 허용한 액션 말고는 그대로 거부된다
			AssertDenied(() => altClient.PutObject(bucketName, "new", body: "x"));
			AssertDenied(() => altClient.ListObjectsV2(bucketName));
			AssertDenied(() => altClient.GetObjectACL(bucketName, key));
			AssertDenied(() => altClient.GetObjectTagging(bucketName, key));
			AssertDenied(() => altClient.DeleteObject(bucketName, key));
			// 목록 권한이 없으면 없는 키도 403 이다
			AssertDenied(() => altClient.GetObject(bucketName, "not-exist"));
			// 정책에 없는 주체
			AssertDenied(() => GetPublicClient().GetObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Effect")]
		[Trait(MainData.Explanation, "Allow 와 Deny 가 겹치면 Deny 가 우선하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyDenyOverridesAllow()
		{
			TestId = 32;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["public/a", "private/a"], bucketName);
			PutPolicy(client, bucketName,
				Statement("Allow", "s3:GetObject", ObjectArn(bucketName)),
				Statement("Deny", "s3:GetObject", ObjectArn(bucketName, "private/*")));
			var altClient = GetAltClient();
			Assert.Equal("public/a", GetBody(altClient.GetObject(bucketName, "public/a")));
			AssertDenied(() => altClient.GetObject(bucketName, "private/a"));
			// Deny 의 주체가 아닌 소유자는 읽을 수 있다
			Assert.Equal("private/a", GetBody(GetClient().GetObject(bucketName, "private/a")));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Effect")]
		[Trait(MainData.Explanation, "명시적 Deny 가 버킷 소유자에게도 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyDenyOwner()
		{
			TestId = 33;
			var key = "TestBucketPolicyDenyOwner";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			PutPolicy(client, bucketName, Statement("Deny", "s3:GetObject", ObjectArn(bucketName), principal: "*"));
			// 명시적 Deny 는 버킷 소유자에게도 적용된다
			var owner = GetClient();
			AssertDenied(() => owner.GetObject(bucketName, key));
			AssertS3Error(HttpStatusCode.Forbidden, null, () => owner.HeadObject(bucketName, key));
			AssertDenied(() => owner.CopyObject(bucketName, key, bucketName, "copy"));
			// 거부하지 않은 액션은 그대로 된다
			owner.PutObject(bucketName, "new", body: "x");
			AssertEmptyList(owner.GetObjectTagging(bucketName, key).Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "NotAction")]
		[Trait(MainData.Explanation, "Deny 문장의 NotAction 이 나열하지 않은 액션만 거부하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyNotActionDeny()
		{
			TestId = 34;
			var key = "TestBucketPolicyNotActionDeny";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			PutPolicy(client, bucketName,
				Statement("Allow", "s3:*", BucketAndObjectArn(bucketName)),
				// GetObject, ListBucket 이 아닌 모든 액션을 거부
				With(Statement("Deny", null, BucketAndObjectArn(bucketName)), "NotAction", new JArray() { "s3:GetObject", "s3:ListBucket" }));
			var altClient = GetAltClient();
			Assert.Equal("content", GetBody(altClient.GetObject(bucketName, key)));
			Assert.Equal(1, altClient.ListObjectsV2(bucketName).KeyCount);
			AssertDenied(() => altClient.PutObject(bucketName, "new", body: "x"));
			AssertDenied(() => altClient.DeleteObject(bucketName, key));
			AssertDenied(() => altClient.GetObjectTagging(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "NotAction")]
		[Trait(MainData.Explanation, "Allow 문장의 NotAction 이 나열하지 않은 액션만 허용하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyNotActionAllow()
		{
			TestId = 35;
			var key = "TestBucketPolicyNotActionAllow";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			// PutObject, DeleteObject 가 아닌 모든 오브젝트 액션을 허용
			PutPolicy(client, bucketName,
				With(Statement("Allow", null, ObjectArn(bucketName)), "NotAction", new JArray() { "s3:PutObject", "s3:DeleteObject" }));
			var altClient = GetAltClient();
			Assert.Equal("content", GetBody(altClient.GetObject(bucketName, key)));
			AssertEmptyList(altClient.GetObjectTagging(bucketName, key).Tagging);
			AssertDenied(() => altClient.PutObject(bucketName, "new", body: "x"));
			AssertDenied(() => altClient.DeleteObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "NotResource")]
		[Trait(MainData.Explanation, "Allow 문장의 NotResource 가 나열하지 않은 리소스만 허용하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyNotResourceAllow()
		{
			TestId = 36;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["public/a", "private/a"], bucketName);
			PutPolicy(client, bucketName,
				With(Statement("Allow", "s3:GetObject", null), "NotResource", ObjectArn(bucketName, "private/*")));
			var altClient = GetAltClient();
			Assert.Equal("public/a", GetBody(altClient.GetObject(bucketName, "public/a")));
			AssertDenied(() => altClient.GetObject(bucketName, "private/a"));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "NotResource")]
		[Trait(MainData.Explanation, "Deny 문장의 NotResource 가 나열하지 않은 리소스만 거부하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyNotResourceDeny()
		{
			TestId = 37;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["public/a", "private/a"], bucketName);
			PutPolicy(client, bucketName,
				Statement("Allow", "s3:GetObject", ObjectArn(bucketName)),
				// public/ 아래가 아닌 것은 모두 거부
				With(Statement("Deny", "s3:GetObject", null), "NotResource", ObjectArn(bucketName, "public/*")));
			var altClient = GetAltClient();
			Assert.Equal("public/a", GetBody(altClient.GetObject(bucketName, "public/a")));
			AssertDenied(() => altClient.GetObject(bucketName, "private/a"));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "NotPrincipal")]
		[Trait(MainData.Explanation, "NotPrincipal 은 Deny 에만 쓸 수 있고 나열하지 않은 주체를 거부하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyNotPrincipal()
		{
			TestId = 38;
			var key = "TestBucketPolicyNotPrincipal";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			var resource = ObjectArn(bucketName);
			// Allow 에는 NotPrincipal 을 쓸 수 없다
			AssertMalformedPolicy(client, bucketName, MakePolicyDocument(
				With(With(Statement("Allow", "s3:GetObject", resource), MainData.PolicyPrincipal, null), MainData.PolicyNotPrincipal, AltPrincipal())));
			PutPolicy(client, bucketName,
				Statement("Allow", "s3:GetObject", resource),
				// 서브 사용자가 아닌 모든 주체의 GetObject 를 거부(소유자 포함)
				With(With(Statement("Deny", "s3:GetObject", resource), MainData.PolicyPrincipal, null), MainData.PolicyNotPrincipal, AltPrincipal()));
			Assert.Equal("content", GetBody(GetAltClient().GetObject(bucketName, key)));
			AssertDenied(() => GetClient().GetObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Wildcard")]
		[Trait(MainData.Explanation, "리소스의 와일드카드(*, ?)가 올바르게 맞춰지는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyResourceWildcard()
		{
			TestId = 39;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["a1/file1.txt", "a1/file10.txt", "b1/file1.txt", "a1/sub/file2.txt", "a1/file1.TXT"], bucketName);
			// * 는 0자 이상(/ 포함), ? 는 정확히 한 글자, 대소문자를 구분한다
			PutPolicy(client, bucketName, Statement("Allow", "s3:GetObject", ObjectArn(bucketName, "a*/file?.txt")));
			var altClient = GetAltClient();
			foreach (var key in new[] { "a1/file1.txt", "a1/sub/file2.txt" })
				Assert.Equal(key, GetBody(altClient.GetObject(bucketName, key)));
			foreach (var key in new[] { "a1/file10.txt", "b1/file1.txt", "a1/file1.TXT" })
				AssertDenied(() => altClient.GetObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Wildcard")]
		[Trait(MainData.Explanation, "액션의 와일드카드(s3:Get*)가 올바르게 맞춰지는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyActionWildcard()
		{
			TestId = 40;
			var key = "TestBucketPolicyActionWildcard";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			PutPolicy(client, bucketName, Statement("Allow", "s3:Get*", BucketAndObjectArn(bucketName)));
			var altClient = GetAltClient();
			Assert.Equal("content", GetBody(altClient.GetObject(bucketName, key)));
			Assert.Single(altClient.GetObjectACL(bucketName, key).AccessControlList.Grants);
			AssertEmptyList(altClient.GetObjectTagging(bucketName, key).Tagging);
			Assert.Equal(VersionStatus.Off, altClient.GetBucketVersioning(bucketName).VersioningConfig.Status);
			altClient.GetBucketLocation(bucketName);
			altClient.GetBucketACL(bucketName);
			// Get 으로 시작하지 않는 액션
			AssertDenied(() => altClient.ListObjectsV2(bucketName));
			AssertDenied(() => altClient.PutObject(bucketName, "new", body: "x"));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Wildcard")]
		[Trait(MainData.Explanation, "액션 이름이 대소문자를 구분하지 않는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyActionCaseInsensitive()
		{
			TestId = 41;
			var key = "TestBucketPolicyActionCaseInsensitive";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			// 액션 이름은 대소문자를 구분하지 않는다
			PutPolicy(client, bucketName, Statement("Allow", "s3:getobject", ObjectArn(bucketName)));
			Assert.Equal("content", GetBody(GetAltClient().GetObject(bucketName, key)));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Resource")]
		[Trait(MainData.Explanation, "ListBucket 을 버킷 리소스에 허용하면 목록 조회만 허용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyListBucketResource()
		{
			TestId = 42;
			var key = "TestBucketPolicyListBucketResource";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			PutPolicy(client, bucketName, Statement("Allow", "s3:ListBucket", S3Utils.MakeArnResource(bucketName)));
			var altClient = GetAltClient();
			Assert.Equal(1, altClient.ListObjectsV2(bucketName).KeyCount);
			Assert.Single(altClient.ListObjects(bucketName).S3Objects);
			altClient.HeadBucket(bucketName);
			// ListBucket 만으로는 읽을 수 없고, 버전·멀티파트 목록은 다른 액션이다
			AssertDenied(() => altClient.GetObject(bucketName, key));
			AssertDenied(() => altClient.ListVersions(bucketName, useLegacyMaxKeys: false));
			AssertDenied(() => altClient.ListMultipartUploads(bucketName));
			// 목록 권한이 있으면 없는 키는 404 다
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => altClient.GetObject(bucketName, "not-exist"));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "s3:prefix 조건이 요청의 prefix 값과 정확히 맞을 때만 목록 조회를 허용하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyListPrefixCondition()
		{
			TestId = 43;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["public/a", "private/a"], bucketName);
			PutPolicy(client, bucketName, Statement("Allow", "s3:ListBucket", S3Utils.MakeArnResource(bucketName),
				condition: Condition("StringEquals", "s3:prefix", "public/")));
			var altClient = GetAltClient();
			Assert.Equal(1, altClient.ListObjectsV2(bucketName, prefix: "public/").KeyCount);
			Assert.Single(altClient.ListObjects(bucketName, prefix: "public/").S3Objects);
			AssertDenied(() => altClient.ListObjectsV2(bucketName));
			AssertDenied(() => altClient.ListObjectsV2(bucketName, prefix: "public/a"));
			AssertDenied(() => altClient.ListObjectsV2(bucketName, prefix: "private/"));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "조건 연산자가 여럿이면 모두 맞아야 목록 조회를 허용하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyListConditionKeys()
		{
			TestId = 44;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["public/a", "private/a"], bucketName);
			// 조건 연산자가 여럿이면 모두 맞아야 한다
			PutPolicy(client, bucketName, Statement("Allow", "s3:ListBucket", S3Utils.MakeArnResource(bucketName),
				condition: new JObject()
				{
					{ "StringLike", new JObject() { { "s3:prefix", "public/*" } } },
					{ "StringEquals", new JObject() { { "s3:delimiter", "/" } } },
					{ "NumericLessThanEquals", new JObject() { { "s3:max-keys", "10" } } },
				}));
			var altClient = GetAltClient();
			var response = altClient.ListObjectsV2(bucketName, prefix: "public/", delimiter: "/", maxKeys: 10);
			Assert.Equal(1, response.KeyCount);
			AssertDenied(() => altClient.ListObjectsV2(bucketName, prefix: "public/", maxKeys: 10));
			AssertDenied(() => altClient.ListObjectsV2(bucketName, prefix: "public/", delimiter: "/", maxKeys: 11));
			AssertDenied(() => altClient.ListObjectsV2(bucketName, prefix: "public/", delimiter: "/"));
			AssertDenied(() => altClient.ListObjectsV2(bucketName, prefix: "private/", delimiter: "/", maxKeys: 10));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "s3:prefix 조건의 Deny 가 요청의 prefix 값만 보고 판정하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyListDenyPrefix()
		{
			TestId = 45;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["public/a", "private/a"], bucketName);
			var resource = S3Utils.MakeArnResource(bucketName);
			PutPolicy(client, bucketName,
				Statement("Allow", "s3:ListBucket", resource),
				Statement("Deny", "s3:ListBucket", resource, condition: Condition("StringLike", "s3:prefix", "private/*")));
			var altClient = GetAltClient();
			AssertDenied(() => altClient.ListObjectsV2(bucketName, prefix: "private/"));
			// 조건은 요청의 prefix 값만 본다. 응답에 private/ 아래 키가 들어 있어도 막지 않는다
			Assert.Equal(2, altClient.ListObjectsV2(bucketName).KeyCount);
			Assert.Equal(1, altClient.ListObjectsV2(bucketName, prefix: "priv").KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "s3:max-keys 조건이 요청의 max-keys 값으로 판정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyListMaxKeys()
		{
			TestId = 46;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["a"], bucketName);
			PutPolicy(client, bucketName, Statement("Allow", "s3:ListBucket", S3Utils.MakeArnResource(bucketName),
				condition: Condition("NumericLessThanEquals", "s3:max-keys", "10")));
			var altClient = GetAltClient();
			Assert.Equal(1, altClient.ListObjectsV2(bucketName, maxKeys: 10).KeyCount);
			Assert.Single(altClient.ListObjects(bucketName, maxKeys: 5).S3Objects);
			AssertDenied(() => altClient.ListObjectsV2(bucketName, maxKeys: 11));
			// max-keys 를 보내지 않으면 조건 키가 없어 맞지 않는다
			AssertDenied(() => altClient.ListObjectsV2(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "aws:SecureTransport 조건으로 http 요청만 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicySecureTransport()
		{
			TestId = 47;
			var key = "TestBucketPolicySecureTransport";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			PutPolicy(client, bucketName, Statement("Deny", "s3:GetObject", ObjectArn(bucketName), principal: "*",
				condition: Condition("Bool", "aws:SecureTransport", "false")));
			var httpClient = GetClientHttp();
			var httpsClient = GetClientHttps();
			Assert.Equal("content", GetBody(httpsClient.GetObject(bucketName, key)));
			AssertDenied(() => httpClient.GetObject(bucketName, key));
			// 거부한 액션이 아니면 http 로도 된다
			httpClient.PutObject(bucketName, "new", body: "x");
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "aws:SourceIp 조건(IpAddress, NotIpAddress)이 올바르게 판정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicySourceIp()
		{
			TestId = 48;
			var key = "TestBucketPolicySourceIp";
			var client = GetClient();
			// (조건, 서브 사용자의 GetObject 가 되는지). 192.0.2.0/24 는 문서용 대역이라 요청 주소가 될 수 없다
			var cases = new (JObject Condition, bool Allowed)[]
			{
				(Condition("IpAddress", "aws:SourceIp", "0.0.0.0/0"), true),
				(Condition("IpAddress", "aws:SourceIp", "192.0.2.0/24"), false),
				(Condition("NotIpAddress", "aws:SourceIp", "192.0.2.0/24"), true),
				(Condition("NotIpAddress", "aws:SourceIp", "0.0.0.0/0"), false),
				(Condition("IpAddress", "aws:SourceIp", new JArray() { "192.0.2.0/24", "0.0.0.0/0" }), true),
			};
			foreach (var (condition, allowed) in cases)
			{
				var caseBucketName = GetNewBucket(client);
				client.PutObject(caseBucketName, key, body: "content");
				PutPolicy(client, caseBucketName, Statement("Allow", "s3:GetObject", ObjectArn(caseBucketName), condition: condition));
				var altClient = GetAltClient();
				if (allowed)
					Assert.Equal("content", GetBody(altClient.GetObject(caseBucketName, key)));
				else
					AssertDenied(() => altClient.GetObject(caseBucketName, key));
			}

			var bucketName = GetNewBucket(client);
			AssertMalformedPolicy(client, bucketName, MakePolicyDocument(
				Statement("Allow", "s3:GetObject", ObjectArn(bucketName), condition: Condition("IpAddress", "aws:SourceIp", "not-an-ip"))));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "암호화 헤더가 AES256 이 아닌 업로드를 거부하는 정책이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyDenyUnencryptedPut()
		{
			TestId = 49;
			var key = "TestBucketPolicyDenyUnencryptedPut";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutPolicy(client, bucketName, Statement("Deny", "s3:PutObject", ObjectArn(bucketName), principal: "*",
				condition: Condition("StringNotEquals", "s3:x-amz-server-side-encryption", "AES256")));
			var owner = GetClient();
			// 헤더가 없으면 값이 AES256 이 아닌 것으로 본다
			AssertDenied(() => owner.PutObject(bucketName, key, body: "content"));
			owner.PutObject(bucketName, key, body: "content", sseKey: ServerSideEncryptionMethod.AES256);
			// 멀티파트 시작과 복사도 PutObject 액션이다
			AssertDenied(() => owner.InitiateMultipartUpload(bucketName, "multi"));
			AssertDenied(() => owner.CopyObject(bucketName, key, bucketName, "copy"));
			owner.CopyObject(bucketName, key, bucketName, "copy", sseKey: ServerSideEncryptionMethod.AES256);
			Assert.Equal("content", GetBody(owner.GetObject(bucketName, "copy")));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "Null 조건으로 암호화 헤더가 없는 업로드를 거부하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyNullCondition()
		{
			TestId = 50;
			var key = "TestBucketPolicyNullCondition";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 암호화 헤더가 없는(Null) 업로드를 거부
			PutPolicy(client, bucketName, Statement("Deny", "s3:PutObject", ObjectArn(bucketName), principal: "*",
				condition: Condition("Null", "s3:x-amz-server-side-encryption", "true")));
			var owner = GetClient();
			AssertDenied(() => owner.PutObject(bucketName, key, body: "content"));
			owner.PutObject(bucketName, key, body: "content", sseKey: ServerSideEncryptionMethod.AES256);
			Assert.Equal(ServerSideEncryptionMethod.AES256, owner.HeadObject(bucketName, key).ServerSideEncryptionMethod);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버전을 지정한 조회는 GetObject 가 아니라 GetObjectVersion 이 필요한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyGetObjectVersionAction()
		{
			TestId = 51;
			var key = "TestBucketPolicyGetObjectVersionAction";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var version1 = client.PutObject(bucketName, key, body: "1").VersionId;
			var version2 = client.PutObject(bucketName, key, body: "2").VersionId;
			PutPolicy(client, bucketName, Statement("Allow", "s3:GetObject", ObjectArn(bucketName)));
			var altClient = GetAltClient();
			Assert.Equal("2", GetBody(altClient.GetObject(bucketName, key)));
			// 버전을 지정한 조회는 s3:GetObjectVersion 이 필요하다(최신 버전이어도)
			foreach (var versionId in new[] { version1, version2 })
				AssertDenied(() => altClient.GetObject(bucketName, key, versionId: versionId));
			AssertS3Error(HttpStatusCode.Forbidden, null, () => altClient.HeadObject(bucketName, key, versionId: version1));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "s3:VersionId 조건으로 특정 버전의 조회만 허용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyVersionIdCondition()
		{
			TestId = 52;
			var key = "TestBucketPolicyVersionIdCondition";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var version1 = client.PutObject(bucketName, key, body: "1").VersionId;
			var version2 = client.PutObject(bucketName, key, body: "2").VersionId;
			PutPolicy(client, bucketName, Statement("Allow", "s3:GetObjectVersion", ObjectArn(bucketName),
				condition: Condition("StringEquals", "s3:VersionId", version1)));
			var altClient = GetAltClient();
			Assert.Equal("1", GetBody(altClient.GetObject(bucketName, key, versionId: version1)));
			AssertDenied(() => altClient.GetObject(bucketName, key, versionId: version2));
			// 버전을 지정하지 않은 조회는 s3:GetObject 가 필요하다
			AssertDenied(() => altClient.GetObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버전을 지정한 삭제는 DeleteObject 가 아니라 DeleteObjectVersion 이 필요한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyDeleteObjectVersionAction()
		{
			TestId = 53;
			var key = "TestBucketPolicyDeleteObjectVersionAction";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var versionId = client.PutObject(bucketName, key, body: "1").VersionId;
			PutPolicy(client, bucketName, Statement("Allow", "s3:DeleteObject", ObjectArn(bucketName)));
			var altClient = GetAltClient();
			// 버전을 지정한 삭제는 s3:DeleteObjectVersion 이 필요하다
			AssertDenied(() => altClient.DeleteObject(bucketName, key, versionId: versionId));
			var response = DeleteObjectsResponseOf(altClient, bucketName, [new() { Key = key, VersionId = versionId }]);
			AssertEmptyList(response.DeletedObjects);
			Assert.Equal(MainData.ACCESS_DENIED, response.DeleteErrors[0].Code);
			// 버전을 지정하지 않은 삭제(삭제 마커)는 된다
			Assert.Equal("true", altClient.DeleteObject(bucketName, key).DeleteMarker, ignoreCase: true);
			Assert.Equal("1", GetBody(client.GetObject(bucketName, key, versionId: versionId)));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Condition")]
		[Trait(MainData.Explanation, "s3:RequestObjectTagKeys 조건으로 허용한 태그 키만 붙일 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyRequestObjectTagKeys()
		{
			TestId = 54;
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			// 요청의 태그 키가 모두 project, team 안에 있어야 한다
			PutPolicy(client, bucketName, Statement("Allow", new JArray() { "s3:PutObject", "s3:PutObjectTagging" }, ObjectArn(bucketName),
				condition: Condition("ForAllValues:StringEquals", "s3:RequestObjectTagKeys", new JArray() { "project", "team" })));
			var altClient = GetAltClient();
			altClient.PutObject(bucketName, "allowed", body: "x", tagSet: [new() { Key = "project", Value = "a" }]);
			// 태그가 없으면 ForAllValues 는 참이다
			altClient.PutObject(bucketName, "no-tag", body: "x");
			AssertDenied(() => altClient.PutObject(bucketName, "denied", body: "x",
				tagSet: [new() { Key = "project", Value = "a" }, new() { Key = "owner", Value = "b" }]));
			var tagSet = client.GetObjectTagging(bucketName, "allowed").Tagging;
			Assert.Single(tagSet);
			Assert.Equal("project", tagSet[0].Key);
			Assert.Equal("a", tagSet[0].Value);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Action")]
		[Trait(MainData.Explanation, "PutObject 액션이 허용하는 범위(업로드, 멀티파트)를 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyPutObjectAction()
		{
			TestId = 55;
			var key = "TestBucketPolicyPutObjectAction";
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			client.PutObject(bucketName, key, body: "content");
			PutPolicy(client, bucketName, Statement("Allow", "s3:PutObject", ObjectArn(bucketName)));
			var altClient = GetAltClient();
			altClient.PutObject(bucketName, "new", body: "x");
			// 멀티파트 시작·파트 업로드는 PutObject 로, 시작한 사용자는 파트 목록·중단도 할 수 있다
			var uploadId = altClient.InitiateMultipartUpload(bucketName, "multi").UploadId;
			altClient.UploadPart(bucketName, "multi", uploadId, "x", 1);
			Assert.Single(altClient.ListParts(bucketName, "multi", uploadId).Parts);
			altClient.AbortMultipartUpload(bucketName, "multi", uploadId);

			// 태그를 붙이려면 PutObjectTagging, 복사하려면 원본의 GetObject 가 필요하다
			AssertDenied(() => altClient.PutObject(bucketName, "tagged", body: "x", tagSet: [new() { Key = "a", Value = "b" }]));
			AssertDenied(() => altClient.CopyObject(bucketName, key, bucketName, "copy"));
			AssertDenied(() => altClient.ListMultipartUploads(bucketName));
			AssertDenied(() => altClient.GetObject(bucketName, "new"));
			AssertDenied(() => altClient.DeleteObject(bucketName, "new"));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Action")]
		[Trait(MainData.Explanation, "멀티파트 목록 액션(ListMultipartUploadParts, ListBucketMultipartUploads)만 허용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyMultipartListActions()
		{
			TestId = 56;
			var key = "TestBucketPolicyMultipartListActions";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;
			client.UploadPart(bucketName, key, uploadId, "x", 1);
			PutPolicy(client, bucketName, Statement("Allow",
				new JArray() { "s3:ListMultipartUploadParts", "s3:ListBucketMultipartUploads" }, BucketAndObjectArn(bucketName)));
			var altClient = GetAltClient();
			Assert.Single(altClient.ListParts(bucketName, key, uploadId).Parts);
			Assert.Single(altClient.ListMultipartUploads(bucketName).MultipartUploads);
			AssertDenied(() => altClient.AbortMultipartUpload(bucketName, key, uploadId));
			AssertDenied(() => altClient.UploadPart(bucketName, key, uploadId, "x", 2));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Action")]
		[Trait(MainData.Explanation, "버킷 설정 액션을 허용하면 그 설정의 조회, 변경, 삭제만 허용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyBucketConfigActions()
		{
			TestId = 57;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var actions = new JArray()
			{
				"s3:GetBucketVersioning",
				"s3:GetBucketTagging",
				"s3:PutBucketTagging",
				"s3:GetBucketCORS",
				"s3:PutBucketCORS",
				"s3:GetBucketAcl",
				"s3:GetBucketLocation",
				"s3:ListBucketVersions",
			};
			PutPolicy(client, bucketName, Statement("Allow", actions, S3Utils.MakeArnResource(bucketName)));
			var altClient = GetAltClient();
			Assert.Equal(VersionStatus.Off, altClient.GetBucketVersioning(bucketName).VersioningConfig.Status);
			altClient.GetBucketACL(bucketName);
			altClient.GetBucketLocation(bucketName);
			AssertEmptyList(altClient.ListVersions(bucketName, useLegacyMaxKeys: false).Versions);
			// 권한은 있고 설정이 없으면 403 이 아니라 404 다
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_TAG_SET, () => altClient.GetBucketTagging(bucketName));
			CheckErrorResponse(HttpStatusCode.NotFound, () => altClient.GetCORSConfiguration(bucketName), MainData.NO_SUCH_CORS_CONFIGURATION);
			// 설정 삭제는 Put 액션으로 판정한다
			altClient.PutBucketTagging(bucketName, [new() { Key = "a", Value = "b" }]);
			var tagSet = client.GetBucketTagging(bucketName).TagSet;
			Assert.Single(tagSet);
			Assert.Equal("a", tagSet[0].Key);
			Assert.Equal("b", tagSet[0].Value);
			altClient.DeleteBucketTagging(bucketName);
			altClient.PutCORSConfiguration(bucketName, new CORSConfiguration()
			{
				Rules = [new() { AllowedMethods = ["GET"], AllowedOrigins = ["*"] }],
			});
			altClient.DeleteCORSConfiguration(bucketName);

			// 허용하지 않은 액션
			AssertDenied(() => altClient.ListObjectsV2(bucketName));
			AssertDenied(() => altClient.PutBucketVersioning(bucketName, status: VersionStatus.Enabled));
			AssertDenied(() => altClient.GetLifecycleConfiguration(bucketName));
			AssertDenied(() => altClient.DeleteBucket(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Form")]
		[Trait(MainData.Explanation, "정책 문서의 여러 표기(단일 문장, Version 생략, Id·Sid, 주체 목록)를 받아들이는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyStatementForms()
		{
			TestId = 58;
			var key = "TestBucketPolicyStatementForms";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			var resource = ObjectArn(bucketName);
			var statement = Statement("Allow", "s3:GetObject", resource);

			// Statement 는 배열이 아니라 객체 하나여도 된다
			client.PutBucketPolicy(bucketName, new JObject()
			{
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, statement },
			}.ToString(Newtonsoft.Json.Formatting.None));
			Assert.Equal("content", GetBody(GetAltClient().GetObject(bucketName, key)));

			// Version 을 빼면 2008-10-17 로 저장된다
			client.PutBucketPolicy(bucketName, new JObject()
			{
				{ MainData.PolicyStatement, new JArray() { statement } },
			}.ToString(Newtonsoft.Json.Formatting.None));
			Assert.Equal("2008-10-17", (string)JObject.Parse(client.GetBucketPolicy(bucketName).Policy)[MainData.PolicyVersion]);
			client.PutBucketPolicy(bucketName, MakePolicyDocument("2008-10-17", statement));

			// Id 와 Sid 는 그대로 돌려준다. Sid 가 겹쳐도 받는다
			var policy = new JObject()
			{
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ "Id", "policy-id" },
				{ MainData.PolicyStatement, new JArray()
					{
						With(Statement("Allow", "s3:GetObject", resource), "Sid", "same"),
						With(Statement("Allow", "s3:GetObjectTagging", resource), "Sid", "same"),
					}
				},
			};
			client.PutBucketPolicy(bucketName, policy.ToString(Newtonsoft.Json.Formatting.None));
			var actual = JObject.Parse(client.GetBucketPolicy(bucketName).Policy);
			Assert.Equal("policy-id", (string)actual["Id"]);
			var statements = (JArray)actual[MainData.PolicyStatement];
			Assert.Equal(2, statements.Count);
			Assert.Equal("same", (string)statements[0]["Sid"]);
			Assert.Equal("same", (string)statements[1]["Sid"]);
			Assert.Equal("s3:GetObject", (string)statements[0][MainData.PolicyAction]);
			Assert.Equal("s3:GetObjectTagging", (string)statements[1][MainData.PolicyAction]);

			// 주체를 여럿 적을 수 있다
			var principal = new JObject() { { "CanonicalUser", new JArray() { Config.AltUser.UserId, Config.MainUser.UserId } } };
			PutPolicy(client, bucketName, Statement("Allow", "s3:GetObject", resource, principal: principal));
			Assert.Equal("content", GetBody(GetAltClient().GetObject(bucketName, key)));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "문장의 항목이 겹치거나 빠지거나 값이 잘못된 정책을 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketPolicyInvalidStatement()
		{
			TestId = 59;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var resource = ObjectArn(bucketName);
			var invalidStatements = new List<JObject>()
			{
				// 같은 뜻의 항목을 함께 쓴다
				With(Statement("Allow", "s3:GetObject", resource), "NotAction", "s3:PutObject"),
				With(Statement("Allow", "s3:GetObject", resource), "NotResource", ObjectArn(bucketName, "a/*")),
				With(Statement("Deny", "s3:GetObject", resource), MainData.PolicyNotPrincipal, AltPrincipal()),
				// 필수 항목 누락
				Statement("Allow", null, resource),
				With(Statement("Allow", "s3:GetObject", resource), MainData.PolicyEffect, null),
				new JObject(),
				// 값이 잘못됐다
				With(Statement("Allow", "s3:GetObject", resource), "Unknown", "value"),
				Statement("Allow", new JArray(), resource),
				Statement("Allow", "ec2:RunInstances", resource),
				Statement("Allow", "s3:GetObject", "*"),
				Statement("Allow", "s3:GetObject", resource, principal: new JObject() { { "CanonicalUser", new string('0', 64) } }),
			};
			foreach (var statement in invalidStatements)
				AssertMalformedPolicy(client, bucketName, MakePolicyDocument(statement));
			// 최상위가 객체가 아니다
			AssertMalformedPolicy(client, bucketName, "[]");
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketPolicy(bucketName), MainData.NO_SUCH_BUCKET_POLICY);
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "정책을 삭제하면 허용했던 권한이 사라지는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyDeleteRevokes()
		{
			TestId = 60;
			var key = "TestBucketPolicyDeleteRevokes";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "content");
			PutPolicy(client, bucketName, Statement("Allow", "s3:GetObject", ObjectArn(bucketName)));
			Assert.Equal("content", GetBody(GetAltClient().GetObject(bucketName, key)));

			client.DeleteBucketPolicy(bucketName);
			AssertDenied(() => GetAltClient().GetObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Effect")]
		[Trait(MainData.Explanation, "다른 사용자가 정책으로 올린 오브젝트를 버킷 소유자와 그 사용자가 읽고 지울 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyOtherUserWrite()
		{
			TestId = 61;
			var key = "TestBucketPolicyOtherUserWrite";
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			PutPolicy(client, bucketName, Statement("Allow",
				new JArray() { "s3:PutObject", "s3:GetObject", "s3:DeleteObject" }, ObjectArn(bucketName)));
			var altClient = GetAltClient();
			altClient.PutObject(bucketName, key, body: "content");
			// 다른 사용자가 올린 오브젝트도 버킷 소유자의 것이다
			Assert.Equal("content", GetBody(client.GetObject(bucketName, key)));
			Assert.Equal("content", GetBody(altClient.GetObject(bucketName, key)));
			var response = DeleteObjectsResponseOf(altClient, bucketName, [new() { Key = key }, new() { Key = "not-exist" }]);
			var deleted = response.DeletedObjects.ConvertAll(d => d.Key);
			deleted.Sort(string.CompareOrdinal);
			var expected = new List<string>() { key, "not-exist" };
			expected.Sort(string.CompareOrdinal);
			Assert.Equal(expected, deleted);
			AssertEmptyList(response.DeleteErrors);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => client.GetObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Policy")]
		[Trait(MainData.Minor, "Effect")]
		[Trait(MainData.Explanation, "모든 주체에게 특정 prefix 의 GetObject 만 허용하면 익명 사용자가 그 범위만 읽을 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyAnonymousPrefix()
		{
			TestId = 62;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			SetupObjects(["public/a", "private/a"], bucketName);
			PutPolicy(client, bucketName, Statement("Allow", "s3:GetObject", ObjectArn(bucketName, "public/*"), principal: "*"));
			Assert.True(client.GetBucketPolicyStatus(bucketName).PolicyStatus.IsPublic);

			var publicClient = GetPublicClient();
			Assert.Equal("public/a", GetBody(publicClient.GetObject(bucketName, "public/a")));
			AssertDenied(() => publicClient.GetObject(bucketName, "private/a"));
			AssertDenied(() => publicClient.PutObject(bucketName, "public/b", body: "x"));
			AssertDenied(() => publicClient.ListObjectsV2(bucketName));
		}

		#endregion
	}
}
