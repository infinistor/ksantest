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
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
{
	public class ListBuckets : TestBase
	{
		public ListBuckets(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_buckets_create_then_list")]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "여러개의 버킷 생성해서 목록 조회 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_buckets_create_then_list()
		{
			var Client = GetClient();
			var BucketNames = new List<string>();
			for (int i = 0; i < 5; i++)
			{
				var BucketName = GetNewBucketName();
				Client.PutBucket(BucketName);
				BucketNames.Add(BucketName);
			}

			var Response = Client.ListBuckets();
			var BucketList = GetBucketList(Response);

			foreach (var BucketName in BucketNames)
			{
				if (!BucketList.Contains(BucketName))
					throw new Exception(string.Format("S3 implementation's GET on Service did not return bucket we created: {0}", BucketName));
			}
		}

		[Fact(DisplayName = "test_list_buckets_invalid_auth")]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 사용자가 버킷목록 조회시 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_list_buckets_invalid_auth()
		{
			var BadAuthClient = GetBadAuthClient();
			var e = Assert.Throws<AggregateException>(() => BadAuthClient.ListBuckets());
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.InvalidAccessKeyId, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_list_buckets_bad_auth")]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "로그인정보를 잘못입력한 사용자가 버킷목록 조회시 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_list_buckets_bad_auth()
		{
			var MainAccessKey = Config.MainUser.AccessKey;
			var BadAuthClient = GetBadAuthClient(AccessKey: MainAccessKey);

			var e = Assert.Throws<AggregateException>(() => BadAuthClient.ListBuckets());
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.SignatureDoesNotMatch, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_head_bucket", Skip = "netcore에서는 HeadBucket API가 존재하지 않음")]
		[Trait(MainData.Major, "ListBuckets")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "버킷의 메타데이터를 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_head_bucket()
		{
		}
	}
}
