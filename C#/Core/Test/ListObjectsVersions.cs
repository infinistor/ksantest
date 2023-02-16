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
using System.Collections.Generic;
using Xunit;

namespace s3tests
{
	public class ListObjectsVersions : TestBase
	{
		public ListObjectsVersions(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_bucket_list_return_data_versioning")]
		[Trait(MainData.Major, "ListObjectsVersions")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "Version정보를 가질 수 있는 버킷에서 ListObjectsVersions로 가져온 Metadata와 " +
									 "HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_return_data_versioning()
		{
			var BucketName = GetNewBucket();
			CheckConfigureVersioningRetry(BucketName, Amazon.S3.VersionStatus.Enabled);
			var KeyNames = new List<string>() { "bar", "baz", "foo" };
			BucketName = SetupObjects(KeyNames, BucketName: BucketName);

			var Client = GetClient();
			var Data = new List<ObjectData>();


			foreach (var Key in KeyNames)
			{
				var ObjResponse = Client.GetObjectMetadata(BucketName, Key);
				var ACLResponse = Client.GetObjectACL(BucketName, Key);

				Data.Add(new ObjectData()
				{
					Key = Key,
					DisplayName = ACLResponse.AccessControlList.Owner.DisplayName,
					Id = ACLResponse.AccessControlList.Owner.Id,
					ETag = ObjResponse.ETag,
					LastModified = ObjResponse.LastModified,
					ContentLength = ObjResponse.ContentLength,
					VersionId = ObjResponse.VersionId
				});
			}

			var Response = Client.ListVersions(BucketName);
			var ObjList = Response.Versions;

			foreach (var Object in ObjList)
			{
				var KeyName = Object.Key;
				var KeyData = GetObjectToKey(KeyName, Data);

				Assert.NotNull(KeyData);
				Assert.Equal(KeyData.ETag, Object.ETag);
				Assert.Equal(KeyData.ContentLength, Object.Size);
				Assert.Equal(KeyData.DisplayName, Object.Owner.DisplayName);
				Assert.Equal(KeyData.Id, Object.Owner.Id);
				Assert.Equal(KeyData.VersionId, Object.VersionId);
				Assert.Equal(KeyData.LastModified, Object.LastModified.ToUniversalTime());
			}
		}
	}
}
