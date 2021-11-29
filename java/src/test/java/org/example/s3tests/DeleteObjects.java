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
package org.example.s3tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;

public class DeleteObjects extends TestBase{
    @Test
	@DisplayName("test_multi_object_delete")
    @Tag("ListObject")
    @Tag("KSAN")
    //@Tag("버킷에 존재하는 오브젝트 여러개를 한번에 삭제")
    public void test_multi_object_delete()
    {
        var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "key0", "key1", "key2" }));
        var BucketName = CreateObjects(KeyNames);
        var Client = GetClient();

        var ListResponse = Client.listObjects(BucketName);
        assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

        var ObjectList = GetKeyVersions(KeyNames);
        var DelResponse = Client.deleteObjects(new DeleteObjectsRequest(BucketName).withKeys(ObjectList));

        assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());
        
        ListResponse = Client.listObjects(BucketName);
        assertEquals(0, ListResponse.getObjectSummaries().size());

        DelResponse = Client.deleteObjects(new DeleteObjectsRequest(BucketName).withKeys(ObjectList));
        assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

        ListResponse = Client.listObjects(BucketName);
        assertEquals(0, ListResponse.getObjectSummaries().size());
    }

    @Test
	@DisplayName("test_multi_objectv2_delete")
    @Tag("ListObjectsV2")
    @Tag("KSAN")
    //@Tag("버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)")
    public void test_multi_objectv2_delete()
    {
        var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "key0", "key1", "key2" }));
        var BucketName = CreateObjects(KeyNames);
        var Client = GetClient();

        var ListResponse = Client.listObjectsV2(BucketName);
        assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

        var ObjectList = GetKeyVersions(KeyNames);
        var DelResponse = Client.deleteObjects(new DeleteObjectsRequest(BucketName).withKeys(ObjectList));

        assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

        ListResponse = Client.listObjectsV2(BucketName);
        assertEquals(0, ListResponse.getObjectSummaries().size());

        DelResponse = Client.deleteObjects(new DeleteObjectsRequest(BucketName).withKeys(ObjectList));
        assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

        ListResponse = Client.listObjectsV2(BucketName);
        assertEquals(0, ListResponse.getObjectSummaries().size());
    }

    @Test
	@DisplayName("test_multi_object_delete")
    @Tag("quiet")
    @Tag("KSAN")
    //@Tag("quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제")
    public void test_multi_object_delete_quiet()
    {
        var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "key0", "key1", "key2" }));
        var BucketName = CreateObjects(KeyNames);
        var Client = GetClient();

        var ListResponse = Client.listObjects(BucketName);
        assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

        var ObjectList = GetKeyVersions(KeyNames);
        var DelResponse = Client.deleteObjects(new DeleteObjectsRequest(BucketName).withKeys(ObjectList).withQuiet(true));

        assertEquals(0, DelResponse.getDeletedObjects().size());
        
        ListResponse = Client.listObjects(BucketName);
        assertEquals(0, ListResponse.getObjectSummaries().size());
    }

    
    @Test
	@DisplayName("test_directory_delete")
    @Tag("Directory")
    @Tag("KSAN")
    //@Tag("업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인")
    public void test_directory_delete()
    {
        var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "a/b/", "a/b/c/d/obj1", "a/b/c/d/obj2", "1/2/", "1/2/3/4/obj1", "q/w/e/r/obj" }));
        var BucketName = CreateObjectsToBody(KeyNames, "");
        var Client = GetClient();

        var ListResponse = Client.listObjects(BucketName);
        assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

        Client.deleteObject(BucketName, "a/b/");
        Client.deleteObject(BucketName, "1/2/");
        Client.deleteObject(BucketName, "q/w/");

        ListResponse = Client.listObjects(BucketName);
        assertEquals(4, ListResponse.getObjectSummaries().size());

        Client.deleteObject(BucketName, "a/b/");
        Client.deleteObject(BucketName, "1/2/");
        Client.deleteObject(BucketName, "q/w/");

        ListResponse = Client.listObjects(BucketName);
        assertEquals(4, ListResponse.getObjectSummaries().size());
    }
}
