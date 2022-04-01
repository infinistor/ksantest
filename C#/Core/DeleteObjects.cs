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
using System.Collections.Generic;
using Xunit;

namespace s3tests
{
    public class DeleteObjects : TestBase
    {
        public DeleteObjects(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

        [Fact(DisplayName = "test_multi_object_delete")]
        [Trait(MainData.Major, "DeleteObjects")]
        [Trait(MainData.Minor, "ListObject")]
        [Trait(MainData.Explanation, "버킷에 존재하는 오브젝트 여러개를 한번에 삭제")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_multi_object_delete()
        {
            var KeyNames = new List<string>() { "key0", "key1", "key2" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var ListResponse = Client.ListObjects(BucketName);
            Assert.Equal(KeyNames.Count, ListResponse.S3Objects.Count);

            var ObjectList = GetKeyVersions(KeyNames);
            var DelResponse = Client.DeleteObjects(BucketName, ObjectList);

            Assert.Equal(KeyNames.Count, DelResponse.DeletedObjects.Count);
            Assert.Empty(DelResponse.DeleteErrors);

            ListResponse = Client.ListObjects(BucketName);
            Assert.Empty(ListResponse.S3Objects);

            DelResponse = Client.DeleteObjects(BucketName, ObjectList);
            Assert.Equal(KeyNames.Count, DelResponse.DeletedObjects.Count);
            Assert.Empty(DelResponse.DeleteErrors);

            ListResponse = Client.ListObjects(BucketName);
            Assert.Empty(ListResponse.S3Objects);
        }

        [Fact(DisplayName = "test_multi_objectv2_delete")]
        [Trait(MainData.Major, "DeleteObjects")]
        [Trait(MainData.Minor, "ListObjectsV2")]
        [Trait(MainData.Explanation, "버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_multi_objectv2_delete()
        {
            var KeyNames = new List<string>() { "key0", "key1", "key2" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var ListResponse = Client.ListObjectsV2(BucketName);
            Assert.Equal(KeyNames.Count, ListResponse.S3Objects.Count);

            var ObjectList = GetKeyVersions(KeyNames);
            var DelResponse = Client.DeleteObjects(BucketName, ObjectList);

            Assert.Equal(KeyNames.Count, DelResponse.DeletedObjects.Count);
            Assert.Empty(DelResponse.DeleteErrors);

            ListResponse = Client.ListObjectsV2(BucketName);
            Assert.Empty(ListResponse.S3Objects);

            DelResponse = Client.DeleteObjects(BucketName, ObjectList);
            Assert.Equal(KeyNames.Count, DelResponse.DeletedObjects.Count);
            Assert.Empty(DelResponse.DeleteErrors);

            ListResponse = Client.ListObjectsV2(BucketName);
            Assert.Empty(ListResponse.S3Objects);

        }

        [Fact(DisplayName = "test_multi_object_delete_quiet")]
        [Trait(MainData.Major, "DeleteObjects")]
        [Trait(MainData.Minor, "Quiet")]
        [Trait(MainData.Explanation, "quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_multi_object_delete_quiet()
        {
            var KeyNames = new List<string>() { "key0", "key1", "key2" };
            var BucketName = SetupObjects(KeyNames);
            var Client = GetClient();

            var ListResponse = Client.ListObjects(BucketName);
            Assert.Equal(KeyNames.Count, ListResponse.S3Objects.Count);

            var ObjectList = GetKeyVersions(KeyNames);
            var DelResponse = Client.DeleteObjects(BucketName, ObjectList, Quiet: true);

            Assert.Empty(DelResponse.DeletedObjects);

            ListResponse = Client.ListObjects(BucketName);
            Assert.Empty(ListResponse.S3Objects);
        }

        [Fact(DisplayName = "test_directory_delete")]
        [Trait(MainData.Major, "DeleteObjects")]
        [Trait(MainData.Minor, "Directory")]
        [Trait(MainData.Explanation, "업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_directory_delete()
        {
            var KeyNames = new List<string>() { "a/b/", "a/b/c/d/obj1", "a/b/c/d/obj2", "1/2/", "1/2/3/4/obj1", "q/w/e/r/obj" };
            var BucketName = SetupObjects(KeyNames, Body: "");
            var Client = GetClient();

            var ListResponse = Client.ListObjects(BucketName);
            Assert.Equal(KeyNames.Count, ListResponse.S3Objects.Count);

            Client.DeleteObject(BucketName, "a/b/");
            Client.DeleteObject(BucketName, "1/2/");
            Client.DeleteObject(BucketName, "q/w/");

            ListResponse = Client.ListObjects(BucketName);
            Assert.Equal(4, ListResponse.S3Objects.Count);

            Client.DeleteObject(BucketName, "a/b/");
            Client.DeleteObject(BucketName, "1/2/");
            Client.DeleteObject(BucketName, "q/w/");

            ListResponse = Client.ListObjects(BucketName);
            Assert.Equal(4, ListResponse.S3Objects.Count);
        }
    }
}
