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
	public class DeleteBucket : TestBase
	{
		public DeleteBucket(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷을 삭제하려 했을 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketDeleteNotExist()
		{
			var bucketName = GetNewBucketName(false);
			var client = GetClient();

			var exception = Assert.Throws<AggregateException>(() => client.DeleteBucket(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(exception));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(exception));
		}
		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "내용이 비어있지 않은 버킷을 삭제하려 했을 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketDeleteNonempty()
		{
			var bucketName = SetupObjects(new List<string>() { "foo" });
			var client = GetClient();

			var exception = Assert.Throws<AggregateException>(() => client.DeleteBucket(bucketName));
			Assert.Equal(HttpStatusCode.Conflict, GetStatus(exception));
			Assert.Equal(MainData.BUCKET_NOT_EMPTY, GetErrorCode(exception));
		}

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "이미 삭제된 버킷을 다시 삭제 시도할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateDelete()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.DeleteBucket(bucketName);

			var exception = Assert.Throws<AggregateException>(() => client.DeleteBucket(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(exception));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(exception));
		}
	}
}
