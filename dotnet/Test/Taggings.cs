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
using Amazon.S3.Model;
using Newtonsoft.Json.Linq;
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Xunit;
using Amazon.S3;

namespace s3tests.Test
{
	public class Taggings : TestBase
	{
		public Taggings(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Tagging")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷에 사용자 추가 태그값을 설정할경우 성공확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestSetTagging()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Tags = new List<Tag>() { new() { Key = "Hello", Value = "World" } };

			var e = Assert.Throws<AggregateException>(() => client.GetBucketTagging(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_TAG_SET, GetErrorCode(e));

			client.PutBucketTagging(bucketName, Tags);

			var Response = client.GetBucketTagging(bucketName);
			Assert.Single(Response.TagSet);
			TaggingCompare(Tags, Response.TagSet);

			client.DeleteBucketTagging(bucketName);

			e = Assert.Throws<AggregateException>(() => client.GetBucketTagging(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_TAG_SET, GetErrorCode(e));
		}
		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트에 태그 설정이 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjTagging()
		{
			TestId = 2;
			var Key = "TestGetObjTagging";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = S3Utils.MakeSimpleTagset(2);

			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트에 태그 설정이 올바르게 적용되는지 헤더정보를 통해 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjHeadTagging()
		{
			TestId = 3;
			var Key = "TestGetObjHeadTagging";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();
			var Count = 2;

			var InputTagSet = S3Utils.MakeSimpleTagset(Count);

			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObject(bucketName, Key);
			Assert.Equal(HttpStatusCode.OK, GetResponse.HttpStatusCode);
			Assert.Equal(Count, GetResponse.TagCount);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Max")]
		[Trait(MainData.Explanation, "추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutMaxTags()
		{
			TestId = 4;
			var Key = "TestPutMaxTags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = S3Utils.MakeSimpleTagset(10);

			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Overflow")]
		[Trait(MainData.Explanation, "추가가능한 최대갯수를 넘겨서 태그를 입력할때 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutExcessTags()
		{
			TestId = 5;
			var Key = "TestPutExcessTags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = S3Utils.MakeSimpleTagset(11);

			var e = Assert.Throws<AggregateException>(() => client.PutObjectTagging(bucketName, Key, InputTagSet));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.BAD_REQUEST, GetErrorCode(e));

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			Assert.Empty(GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Max")]
		[Trait(MainData.Explanation, "태그의 key값의 길이가 최대(128) value값의 길이가 최대(256)일때 태그를 입력할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutMaxSizeTags()
		{
			TestId = 6;
			var Key = "TestPutMaxSizeTags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = new Tagging() { TagSet = S3Utils.MakeTagList(128, 256) };

			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Overflow")]
		[Trait(MainData.Explanation, "태그의 key값의 길이가 최대(129) value값의 길이가 최대(256)일때 태그 입력 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutExcessKeyTags()
		{
			TestId = 7;
			var Key = "TestPutExcessKeyTags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = new Tagging() { TagSet = S3Utils.MakeTagList(129, 256) };

			var e = Assert.Throws<AggregateException>(() => client.PutObjectTagging(bucketName, Key, InputTagSet));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_TAG, GetErrorCode(e));

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			Assert.Empty(GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Overflow")]
		[Trait(MainData.Explanation, "태그의 key값의 길이가 최대(128) value값의 길이가 최대(257)일때 태그 입력 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutExcessValTags()
		{
			TestId = 8;
			var Key = "TestPutExcessValTags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = new Tagging() { TagSet = S3Utils.MakeTagList(128, 257) };

			var e = Assert.Throws<AggregateException>(() => client.PutObjectTagging(bucketName, Key, InputTagSet));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_TAG, GetErrorCode(e));

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			Assert.Empty(GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "오브젝트의 태그목록을 덮어쓰기 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutModifyTags()
		{
			TestId = 9;
			var Key = "TestPutModifyTags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = new Tagging()
			{
				TagSet =
				[
					new(){ Key = "key", Value = "val"},
					new(){ Key = "key2", Value = "val2"},
				]
			};

			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);

			var InputTagSet2 = new Tagging()
			{
				TagSet =
				[
					new(){ Key = "key3", Value = "val3"},
				]
			};

			PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet2);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagSet2.TagSet, GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "오브젝트의 태그를 삭제 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutDeleteTags()
		{
			TestId = 10;
			var Key = "TestPutDeleteTags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = S3Utils.MakeSimpleTagset(2);

			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);

			var DelResponse = client.DeleteObjectTagging(bucketName, Key);
			Assert.Equal(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);

			GetResponse = client.GetObjectTagging(bucketName, Key);
			Assert.Empty(GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "로그인 정보가 있는 Post방식으로 태그정보, ACL을 포함한 오브젝트를 업로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectTagsAuthenticatedRequest()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var ContentType = "text/plain";
			var Key = "TestPostObjectTagsAuthenticatedRequest";

			var InputTagSet = S3Utils.MakeSimpleTagset(2);
			var XmlInputTagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "TestPostObjectTagsAuthenticatedRequest" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
						{ new JArray() { "starts-with", "$tagging", "" } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "tagging", XmlInputTagset },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			Assert.Equal(HttpStatusCode.NoContent, Result.StatusCode);

			var Response = client.GetObject(bucketName, Key);
			var Body = S3Utils.GetBody(Response);
			Assert.Equal("bar", Body);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "PutObject")]
		[Trait(MainData.Explanation, "헤더에 태그정보를 포함한 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjWithTags()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "TestPutObjWithTags";
			var Data = S3Utils.RandomTextToLong(100);

			var TagSet = new List<Tag>()
			{
				new(){ Key = "bar", Value = ""},
				new(){ Key = "foo", Value = "bar"},
			};

			var Headers = new List<KeyValuePair<string, string>>() { new("x-amz-tagging", "foo=bar&bar") };

			client.PutObject(bucketName, Key, body: Data, headerList: Headers);
			var Response = client.GetObject(bucketName, Key);
			var Body = S3Utils.GetBody(Response);
			Assert.Equal(Data, Body);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			var ResponseTagSet = GetResponse.Tagging;

			TaggingCompare(TagSet, ResponseTagSet);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "PutObject")]
		[Trait(MainData.Explanation, "빈 태그 설정으로 오브젝트를 업로드할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjNonTagging()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "TestGetObjNonTagging";

			client.PutObject(bucketName, Key, body: "", tagSet: []);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "PutObject")]
		[Trait(MainData.Explanation, "x-amz-tagging 헤더에 URL 인코딩이 필요한 문자(공백, /, +, =)가 든 태그로 업로드하면 디코딩된 값으로 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjWithTagsEncoded()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestPutObjWithTagsEncoded";

			// 공백, '/', '+', '='는 x-amz-tagging 헤더에서 URL 인코딩되어 전송되므로 서버가 디코딩해서 저장해야 한다
			var tagSet = new List<Tag>()
			{
				new(){ Key = "k 1", Value = "a/b+c d"},
				new(){ Key = "k2", Value = "x=y"},
			};

			client.PutObject(bucketName, key, body: key, tagSet: tagSet);

			var getResponse = client.GetObjectTagging(bucketName, key);
			TaggingCompare(tagSet, getResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "x-amz-tagging 헤더에 URL 인코딩이 필요한 문자가 든 태그로 멀티파트 업로드하면 디코딩된 값으로 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadWithTagsEncoded()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestMultipartUploadWithTagsEncoded";

			var tagSet = new List<Tag>()
			{
				new(){ Key = "k 1", Value = "a/b+c d"},
				new(){ Key = "k2", Value = "x=y"},
			};

			var uploadId = client.InitiateMultipartUpload(bucketName, key, tagSet: tagSet).UploadId;
			var partResponse = client.UploadPart(bucketName, key, uploadId, key, 1);
			client.CompleteMultipartUpload(bucketName, key, uploadId, [new PartETag(1, partResponse.ETag)]);

			var getResponse = client.GetObjectTagging(bucketName, key);
			TaggingCompare(tagSet, getResponse.Tagging);
		}

		private static List<Tag> Tags(params (string Key, string Value)[] pairs)
		{
			var tagSet = new List<Tag>();
			foreach (var (key, value) in pairs) tagSet.Add(new Tag() { Key = key, Value = value });
			return tagSet;
		}

		private static List<Tag> MakeSimpleTagSet(int count)
		{
			var tagSet = new List<Tag>();
			for (int i = 0; i < count; i++) tagSet.Add(new Tag() { Key = $"key{i}", Value = $"value{i}" });
			return tagSet;
		}

		/// <summary>오브젝트의 태그 목록. SDK v4 는 태그가 없으면 null 을 돌려준다.</summary>
		private static List<Tag> GetObjectTags(S3Client client, string bucketName, string key, string versionId = null)
			=> client.GetObjectTagging(bucketName, key, versionId: versionId).Tagging ?? [];

		private static List<KeyValuePair<string, string>> TaggingHeader(string tagging) => [new("x-amz-tagging", tagging)];

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Max")]
		[Trait(MainData.Explanation, "버킷 태그는 50개까지만 넣을 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketTaggingMax()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			// 버킷 태그는 50개까지 넣을 수 있다
			var tagSet = MakeSimpleTagSet(50);
			client.PutBucketTagging(bucketName, tagSet);
			TaggingCompare(tagSet, client.GetBucketTagging(bucketName).TagSet);

			AssertS3Error(HttpStatusCode.BadRequest, MainData.BAD_REQUEST, () => client.PutBucketTagging(bucketName, MakeSimpleTagSet(51)));
			Assert.Equal(50, client.GetBucketTagging(bucketName).TagSet.Count);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷 태그의 키가 중복되면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketTaggingDuplicateKey()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_TAG,
				() => client.PutBucketTagging(bucketName, Tags(("key", "value1"), ("key", "value2"))));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷 태그의 키·값이 규칙에 맞지 않으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketTaggingInvalidTag()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var invalidTags = new List<(string Key, string Value)>()
			{
				("aws:key", "value"), // aws: 접두어는 시스템 태그 전용
				(new string('k', 129), "value"), // 키 128자 초과
				("key", new string('v', 257)), // 값 256자 초과
				("key?<>", "value"), // 허용하지 않는 문자
			};
			foreach (var tag in invalidTags)
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_TAG, () => client.PutBucketTagging(bucketName, Tags(tag)));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_TAG_SET, () => client.GetBucketTagging(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷 태그의 값이 비어 있어도 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketTaggingEmptyValue()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var tagSet = Tags(("key", ""));
			client.PutBucketTagging(bucketName, tagSet);

			var actual = client.GetBucketTagging(bucketName).TagSet;
			Assert.Single(actual);
			Assert.Equal("key", actual[0].Key);
			// SDK v4 는 빈 값을 null 로 돌려줄 수 있다
			Assert.Equal("", actual[0].Value ?? "");
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "빈 태그 목록을 넣으면 버킷 태그가 지워지고, 태그가 없어도 삭제가 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketTaggingEmptySet()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketTagging(bucketName, Tags(("key", "value")));

			// 빈 태그 목록을 넣으면 태그가 지워진다
			client.PutBucketTagging(bucketName, []);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_TAG_SET, () => client.GetBucketTagging(bucketName));

			// 태그가 없어도 삭제는 성공한다
			var response = client.DeleteBucketTagging(bucketName);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 버킷에 태그 조회/설정/삭제를 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketTaggingBucketNotExist()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.GetBucketTagging(bucketName));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.PutBucketTagging(bucketName, Tags(("key", "value"))));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.DeleteBucketTagging(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "오브젝트 태그의 키가 중복되면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjTaggingDuplicateKey()
		{
			TestId = 22;
			var key = "TestObjTaggingDuplicateKey";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "");
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_TAG,
				() => client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = Tags(("key", "value1"), ("key", "value2")) }));
			Assert.Empty(GetObjectTags(client, bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "오브젝트 태그의 키가 규칙에 맞지 않으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjTaggingInvalidTag()
		{
			TestId = 23;
			var key = "TestObjTaggingInvalidTag";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "");
			var invalidTags = new List<(string Key, string Value)>()
			{
				("aws:key", "value"), // aws: 접두어는 시스템 태그 전용
				("key?<>", "value"), // 허용하지 않는 문자
			};
			foreach (var tag in invalidTags)
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_TAG,
					() => client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = Tags(tag) }));
			Assert.Empty(GetObjectTags(client, bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "허용 문자와 빈 값으로 된 오브젝트 태그가 그대로 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjTaggingSpecialCharacters()
		{
			TestId = 24;
			var key = "TestObjTaggingSpecialCharacters";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "");
			// 허용 문자(영숫자, 공백, + - = . _ : / @)와 빈 값은 그대로 저장된다
			var tagSet = Tags(("a+b-=._:/@ c", "v+-=._:/@ d"), ("empty", ""));
			client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = tagSet });

			var actual = GetObjectTags(client, bucketName, key);
			// SDK v4 는 빈 값을 null 로 돌려줄 수 있다
			foreach (var tag in actual) tag.Value ??= "";
			TaggingCompare(tagSet, actual);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "빈 태그 목록을 넣으면 오브젝트 태그가 지워지고, 태그가 없어도 삭제가 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjTaggingEmptySet()
		{
			TestId = 25;
			var key = "TestObjTaggingEmptySet";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: "");
			client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = Tags(("key", "value")) });

			// 빈 태그 목록을 넣으면 태그가 지워진다
			client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = [] });
			Assert.Empty(GetObjectTags(client, bucketName, key));

			// 태그가 없어도 삭제는 성공한다
			var response = client.DeleteObjectTagging(bucketName, key);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 오브젝트에 태그 조회/설정/삭제를 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjTaggingKeyNotExist()
		{
			TestId = 26;
			var key = "TestObjTaggingKeyNotExist";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY,
				() => client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = Tags(("key", "value")) }));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => client.GetObjectTagging(bucketName, key));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => client.DeleteObjectTagging(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Overflow")]
		[Trait(MainData.Explanation, "헤더로 태그를 10개 넘게 주면 오브젝트 업로드가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutObjWithExcessTags()
		{
			TestId = 27;
			var key = "TestPutObjWithExcessTags";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var pairs = new List<string>();
			for (int i = 0; i < 11; i++) pairs.Add($"key{i}=value{i}");
			AssertS3Error(HttpStatusCode.BadRequest, MainData.BAD_REQUEST,
				() => client.PutObject(bucketName, key, body: "", headerList: TaggingHeader(string.Join("&", pairs))));
			// HEAD 응답에는 본문이 없어 상태 코드만 확인한다
			AssertS3Error(HttpStatusCode.NotFound, null, () => client.GetObjectMetadata(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "헤더로 준 태그가 규칙에 맞지 않으면 오브젝트 업로드가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutObjWithInvalidTags()
		{
			TestId = 28;
			var key = "TestPutObjWithInvalidTags";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 헤더로 준 태그의 키가 중복되면 InvalidArgument
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => client.PutObject(bucketName, key, body: "", headerList: TaggingHeader("key=value1&key=value2")));
			foreach (var tagging in new[] { "aws:key=value", "key%3F=value", new string('k', 129) + "=value" })
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_TAG,
					() => client.PutObject(bucketName, key, body: "", headerList: TaggingHeader(tagging)));
			// HEAD 응답에는 본문이 없어 상태 코드만 확인한다
			AssertS3Error(HttpStatusCode.NotFound, null, () => client.GetObjectMetadata(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버전을 지정한 태그 설정/조회/삭제가 그 버전에만 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjTaggingVersion()
		{
			TestId = 29;
			var key = "TestObjTaggingVersion";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var version1 = client.PutObject(bucketName, key, body: "1").VersionId;
			var version2 = client.PutObject(bucketName, key, body: "2").VersionId;
			var tagSet = Tags(("version", "1"));

			// 버전을 지정하면 그 버전의 태그만 바뀐다
			var putResponse = client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = tagSet }, versionId: version1);
			Assert.Equal(version1, putResponse.VersionId);

			var getResponse = client.GetObjectTagging(bucketName, key, versionId: version1);
			Assert.Equal(version1, getResponse.VersionId);
			TaggingCompare(tagSet, getResponse.Tagging);

			// 버전을 지정하지 않으면 최신 버전이 대상이다
			getResponse = client.GetObjectTagging(bucketName, key);
			Assert.Equal(version2, getResponse.VersionId);
			Assert.Empty(getResponse.Tagging ?? []);

			var deleteResponse = client.DeleteObjectTagging(bucketName, key, versionId: version1);
			Assert.Equal(version1, deleteResponse.VersionId);
			Assert.Empty(GetObjectTags(client, bucketName, key, version1));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "잘못된 버전 ID 로 태그 조회/설정/삭제를 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjTaggingInvalidVersion()
		{
			TestId = 30;
			var key = "TestObjTaggingInvalidVersion";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			client.PutObject(bucketName, key, body: "1");
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => client.GetObjectTagging(bucketName, key, versionId: "invalid"));
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = Tags(("key", "value")) }, versionId: "invalid"));
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => client.DeleteObjectTagging(bucketName, key, versionId: "invalid"));
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "최신 버전이 삭제 마커이면 태그 요청이 실패하고, 그 아래 버전은 버전 지정으로 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjTaggingDeleteMarker()
		{
			TestId = 31;
			var key = "TestObjTaggingDeleteMarker";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var versionId = client.PutObject(bucketName, key, body: "1").VersionId;
			client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = Tags(("key", "value")) });
			var deleteMarker = client.DeleteObject(bucketName, key).VersionId;

			// 최신 버전이 삭제 마커이면 버전을 지정하든 않든 태그 요청은 405
			foreach (var target in new[] { null, deleteMarker })
			{
				AssertS3Error(HttpStatusCode.MethodNotAllowed, MainData.METHOD_NOT_ALLOWED,
					() => client.GetObjectTagging(bucketName, key, versionId: target));
				AssertS3Error(HttpStatusCode.MethodNotAllowed, MainData.METHOD_NOT_ALLOWED,
					() => client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = Tags(("key", "value2")) }, versionId: target));
				AssertS3Error(HttpStatusCode.MethodNotAllowed, MainData.METHOD_NOT_ALLOWED,
					() => client.DeleteObjectTagging(bucketName, key, versionId: target));
			}

			// 삭제 마커 아래의 실제 버전은 버전을 지정하면 그대로 조회된다
			TaggingCompare(Tags(("key", "value")), GetObjectTags(client, bucketName, key, versionId));
		}
	}
}
