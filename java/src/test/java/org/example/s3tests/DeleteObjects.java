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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class DeleteObjects {

    org.example.test.DeleteObjects Test = new org.example.test.DeleteObjects();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @Tag("KSAN")
    @Tag("ListObject")
    // @Tag("버킷에 존재하는 오브젝트 여러개를 한번에 삭제")
    public void test_multi_object_delete()
    {
        Test.test_multi_object_delete();
    }

    @Test
    @Tag("KSAN")
    @Tag("ListObjectsV2")
    // @Tag("버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)")
    public void test_multi_objectv2_delete()
    {
        Test.test_multi_objectv2_delete();
    }

    @Test
    @Tag("KSAN")
    @Tag("quiet")
    // @Tag("quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제")
    public void test_multi_object_delete_quiet()
    {
        Test.test_multi_object_delete_quiet();
    }

    @Test
    @Tag("KSAN")
    @Tag("Directory")
    // @Tag("업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인")
    public void test_directory_delete()
    {
        Test.test_directory_delete();
    }
}
