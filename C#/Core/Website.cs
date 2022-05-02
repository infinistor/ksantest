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
using System.Net;
using Xunit;

namespace s3tests
{
	public class Website : TestBase
	{
		public Website(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_webiste_get_buckets")]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 Websize 설정 조회 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_webiste_get_buckets()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Response = Client.GetBucketWebsite(BucketName);
			Assert.Equal(HttpStatusCode.NotFound, Response.HttpStatusCode);
		}

		[Fact(DisplayName = "test_webiste_put_buckets")]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 Websize 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_webiste_put_buckets()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var WebConfig = new WebsiteConfiguration()
			{
				ErrorDocument = "400",
				IndexDocumentSuffix = "a"
			};

			var Response = Client.PutBucketWebsite(BucketName, WebConfig);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var GetResponse = Client.GetBucketWebsite(BucketName);
			Assert.Equal(WebConfig.ErrorDocument, GetResponse.WebsiteConfiguration.ErrorDocument);
		}

		[Fact(DisplayName = "test_webiste_delete_buckets")]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷의 Websize 설정이 삭제가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_webiste_delete_buckets()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var WebConfig = new WebsiteConfiguration()
			{
				ErrorDocument = "400",
				IndexDocumentSuffix = "a"
			};

			var Response = Client.PutBucketWebsite(BucketName, WebConfig);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var DelResponse = Client.DeleteBucketWebsite(BucketName);
			Assert.Equal(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);
		}
	}
}
