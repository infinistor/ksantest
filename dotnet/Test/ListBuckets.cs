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
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class ListBuckets : TestBase
	{
		public ListBuckets(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "여러개의 버킷 생성해서 목록 조회 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketsCreateThenList()
		{
			TestId = 1;
			var Client = GetClient();
			var BucketNames = new List<string>();
			for (int i = 0; i < 5; i++)
			{
				var bucketName = GetNewBucketName();
				Client.PutBucket(bucketName);
				BucketNames.Add(bucketName);
			}

			var Response = Client.ListBuckets();
			var BucketList = GetBucketList(Response);

			foreach (var bucketName in BucketNames)
			{
				if (!BucketList.Contains(bucketName))
					throw new Exception(string.Format("S3 implementation's GET on Service did not return bucket we created: {0}", bucketName));
			}
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "prefix로 버킷 목록을 필터링할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketsPrefix()
		{
			TestId = 5;
			var client = GetClient();
			var prefix = "1111-my-test";
			var bucketName = S3Utils.MakeBucketName(prefix, GetSuiteId(), 5);
			client.PutBucket(bucketName);

			for (int i = 0; i < 5; i++) GetNewBucket(client);

			var response = client.ListBuckets(prefix: prefix);
			var bucketList = GetBucketList(response);
			Assert.Single(bucketList);
			Assert.Equal(bucketName, bucketList[0]);
			client.DeleteBucket(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "MaxBuckets")]
		[Trait(MainData.Explanation, "maxBuckets로 버킷 목록 개수를 제한할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketsMaxBuckets()
		{
			TestId = 6;
			var client = GetClient();
			for (int i = 0; i < 5; i++) GetNewBucket(client);

			// {prefix}{suite}-{testId}- 로 한정해 병렬 실행 시 다른 테스트 버킷과 섞이지 않게 함
			var prefix = GetPrefix() + GetSuiteId() + $"-{TestId}-";

			// 전체 버킷 리스트를 먼저 가져옴
			var fullBucketList = GetBucketList(client.ListBuckets(prefix: prefix));
			fullBucketList.Sort(StringComparer.Ordinal);

			// maxBuckets로 제한해서 가져온 결과가 전체 리스트의 앞부분과 일치하는지 확인
			var bucketList = GetBucketList(client.ListBuckets(prefix: prefix, maxBuckets: 2));
			Assert.Equal(2, bucketList.Count);
			Assert.Equal(fullBucketList.GetRange(0, 2), bucketList);
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "ContinuationToken")]
		[Trait(MainData.Explanation, "continuationToken으로 버킷 목록을 페이징할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketsContinuationToken()
		{
			TestId = 7;
			var client = GetClient();
			for (int i = 0; i < 5; i++) GetNewBucket(client);

			// {prefix}{suite}-{testId}- 로 한정해 병렬 실행 시 다른 테스트 버킷과 섞이지 않게 함
			var prefix = GetPrefix() + GetSuiteId() + $"-{TestId}-";

			// 전체 버킷 리스트를 먼저 가져옴
			var fullBucketList = GetBucketList(client.ListBuckets(prefix: prefix));
			fullBucketList.Sort(StringComparer.Ordinal);

			// 첫 번째 페이지: maxBuckets=2
			var response = client.ListBuckets(prefix: prefix, maxBuckets: 2);
			var bucketList = GetBucketList(response);
			Assert.Equal(2, bucketList.Count);
			Assert.Equal(fullBucketList.GetRange(0, 2), bucketList);

			// 두 번째 페이지: continuationToken 사용
			var response2 = client.ListBuckets(prefix: prefix, maxBuckets: 2, continuationToken: response.ContinuationToken);
			var bucketList2 = GetBucketList(response2);
			Assert.Equal(2, bucketList2.Count);
			Assert.Equal(fullBucketList.GetRange(2, 2), bucketList2);
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 사용자가 버킷목록 조회시 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestListBucketsInvalidAuth()
		{
			TestId = 2;
			var BadAuthClient = GetBadAuthClient();
			var e = Assert.Throws<AggregateException>(() => BadAuthClient.ListBuckets());
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.INVALID_ACCESS_KEY_ID, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "로그인정보를 잘못입력한 사용자가 버킷목록 조회시 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestListBucketsBadAuth()
		{
			TestId = 3;
			var MainAccessKey = Config.MainUser.AccessKey;
			var BadAuthClient = GetBadAuthClient(accessKey: MainAccessKey);

			var e = Assert.Throws<AggregateException>(() => BadAuthClient.ListBuckets());
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.SIGNATURE_DOES_NOT_MATCH, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "버킷의 메타데이터를 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestHeadBucket()
		{
			TestId = 4;
		}

		/// <summary>HEAD 요청은 응답 본문이 없어 에러 코드 대신 상태 코드만 확인한다.</summary>
		private static HttpStatusCode HttpStatus(Action call)
		{
			var e = Assert.Throws<AggregateException>(call);
			return GetStatus(e);
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 메타데이터를 조회하면 404가 반환되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestHeadBucketNotExist()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			Assert.Equal(HttpStatusCode.NotFound, HttpStatus(() => client.HeadBucket(bucketName)));
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "다른 사용자가 버킷의 메타데이터를 조회하면 403이 반환되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestHeadBucketAltUser()
		{
			TestId = 9;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			Assert.Equal(HttpStatusCode.Forbidden, HttpStatus(() => altClient.HeadBucket(bucketName)));
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "버킷의 메타데이터 조회 응답에 버킷의 리전이 포함되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestHeadBucketRegion()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// x-amz-bucket-region 헤더
			var region = client.HeadBucket(bucketName).BucketRegion;
			Assert.False(string.IsNullOrEmpty(region));
			if (!string.IsNullOrEmpty(Config.S3.RegionName))
				Assert.Equal(Config.S3.RegionName, region);
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Prefix")]
		[Trait(MainData.Explanation, "일치하는 버킷이 없는 prefix로 조회하면 빈 목록이 반환되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketsPrefixNotMatch()
		{
			TestId = 11;
			var client = GetClient();
			GetNewBucket(client);
			var response = client.ListBuckets(prefix: "zzzz-no-such-bucket-prefix-" + GetSuiteId());
			Assert.Empty(response.Buckets ?? []);
			Assert.Null(response.ContinuationToken);
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "maxBuckets가 허용 범위를 벗어나면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestListBucketsMaxBucketsInvalid()
		{
			TestId = 12;
			var client = GetClient();
			// max-buckets 는 1 이상 10000 이하만 허용된다
			var e = Assert.Throws<AggregateException>(() => client.ListBuckets(maxBuckets: 10001));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 continuationToken으로 조회하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestListBucketsInvalidContinuationToken()
		{
			TestId = 13;
			var client = GetClient();
			var e = Assert.Throws<AggregateException>(() => client.ListBuckets(continuationToken: "invalid-continuation-token"));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "ContinuationToken")]
		[Trait(MainData.Explanation, "continuationToken으로 끝까지 페이징한 결과가 전체 목록과 순서까지 같은지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketsPaging()
		{
			TestId = 14;
			var client = GetClient();
			for (int i = 0; i < 5; i++) GetNewBucket(client);
			var prefix = GetPrefix() + GetSuiteId() + $"-{TestId}-";
			var fullBucketList = GetBucketList(client.ListBuckets(prefix: prefix));
			Assert.Equal(5, fullBucketList.Count);

			// 토큰이 없어질 때까지 끝까지 넘기면 전체 목록과 순서까지 같다
			var pagedBucketList = new List<string>();
			var pageCount = 0;
			string token = null;
			while (true)
			{
				var response = client.ListBuckets(prefix: prefix, maxBuckets: 2, continuationToken: token);
				var page = (response.Buckets ?? []).ConvertAll(bucket => bucket.BucketName);
				Assert.True(page.Count <= 2);
				pagedBucketList.AddRange(page);
				pageCount++;
				token = response.ContinuationToken;
				if (string.IsNullOrEmpty(token)) break;
			}
			Assert.Equal(3, pageCount);
			Assert.Equal(fullBucketList, pagedBucketList);
			var sortedBucketList = new List<string>(pagedBucketList);
			sortedBucketList.Sort(StringComparer.Ordinal);
			Assert.Equal(sortedBucketList, pagedBucketList);
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷 목록 조회 응답에 버킷 정보와 소유자 정보가 포함되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketsOwner()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var response = client.ListBuckets(prefix: bucketName);
			Assert.Single(response.Buckets);
			Assert.Equal(bucketName, response.Buckets[0].BucketName);
			Assert.NotNull(response.Buckets[0].CreationDate);
			Assert.Equal(Config.MainUser.UserId, response.Owner.Id);
		}

		[Fact]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "다른 사용자의 버킷 목록에는 내 버킷이 나오지 않는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketsAltUser()
		{
			TestId = 16;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			// 다른 사용자의 목록에는 내 버킷이 나오지 않는다
			var response = altClient.ListBuckets(prefix: bucketName);
			Assert.Empty(response.Buckets ?? []);
		}
	}
}
