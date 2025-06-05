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
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Xunit;

namespace s3tests
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Tags = new List<Tag>() { new() { Key = "Hello", Value = "world" } };

			var Response = client.GetBucketTagging(bucketName);
			Assert.Empty(Response.TagSet);

			client.PutBucketTagging(bucketName, Tags);

			Response = client.GetBucketTagging(bucketName);
			Assert.Single(Response.TagSet);
			Assert.Equal(Tags[0].Key, Response.TagSet[0].Key);
			Assert.Equal(Tags[0].Value, Response.TagSet[0].Value);

			client.DeleteBucketTagging(bucketName);

			Response = client.GetBucketTagging(bucketName);
			Assert.Empty(Response.TagSet);

		}
		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트에 태그 설정이 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjTagging()
		{
			var Key = "testputtags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = MakeSimpleTagset(2);

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
		public void test_get_obj_head_tagging()
		{
			var Key = "testputtags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();
			var Count = 2;

			var InputTagSet = MakeSimpleTagset(Count);

			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObject(bucketName, Key);
			Assert.Equal(HttpStatusCode.OK, GetResponse.HttpStatusCode);
			Assert.Equal(Count.ToString(), GetResponse.Headers["x-amz-tagging-count"]);
		}

		[Fact]
		[Trait(MainData.Major, "Taggings")]
		[Trait(MainData.Minor, "Max")]
		[Trait(MainData.Explanation, "추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_put_max_tags()
		{
			var Key = "testputmaxtags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = MakeSimpleTagset(10);

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
		public void test_put_excess_tags()
		{
			var Key = "testputmaxtags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = MakeSimpleTagset(11);

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
		public void test_put_max_kvsize_tags()
		{
			var Key = "testputmaxkeysize";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = new Tagging() { TagSet = MakeTagList(128, 256) };

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
		public void test_put_excess_key_tags()
		{
			var Key = "testputexcesskeytags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = new Tagging() { TagSet = MakeTagList(129, 256) };

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
		public void test_put_excess_val_tags()
		{
			var Key = "testputexcesskeytags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = new Tagging() { TagSet = MakeTagList(128, 257) };

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
		public void test_put_modify_tags()
		{
			var Key = "testputmodifytags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = new Tagging()
			{
				TagSet = new List<Tag>()
				{
					new(){ Key = "key", Value = "val"},
					new(){ Key = "key2", Value = "val2"},
				}
			};

			var PutResponse = client.PutObjectTagging(bucketName, Key, InputTagSet);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			TaggingCompare(InputTagSet.TagSet, GetResponse.Tagging);

			var InputTagSet2 = new Tagging()
			{
				TagSet = new List<Tag>()
				{
					new(){ Key = "key3", Value = "val3"},
				}
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
		public void test_put_delete_tags()
		{
			var Key = "testputmodifytags";
			var bucketName = SetupKeyWithRandomContent(Key);
			var client = GetClient();

			var InputTagSet = MakeSimpleTagset(2);

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
		public void test_post_object_tags_authenticated_request()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var ContentType = "text/plain";
			var Key = "foo.txt";

			var InputTagSet = MakeSimpleTagset(2);
			var XmlInputTagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
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

			var Result = PostUpload(bucketName, Payload);
			Assert.Equal(HttpStatusCode.NoContent, Result.StatusCode);

			var Response = client.GetObject(bucketName, Key);
			var Body = GetBody(Response);
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "testtagobj1";
			var Data = RandomTextToLong(100);

			var TagSet = new List<Tag>()
			{
				new(){ Key = "bar", Value = ""},
				new(){ Key = "foo", Value = "bar"},
			};

			var Headers = new List<KeyValuePair<string, string>>() { new("x-amz-tagging", "foo=bar&bar") };

			client.PutObject(bucketName, Key, body: Data, headerList: Headers);
			var Response = client.GetObject(bucketName, Key);
			var Body = GetBody(Response);
			Assert.Equal(Data, Body);

			var GetResponse = client.GetObjectTagging(bucketName, Key);
			var ResponseTagSet = GetResponse.Tagging;

			TaggingCompare(TagSet, ResponseTagSet);
		}
	}
}
