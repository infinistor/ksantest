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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class LifeCycle
{
    org.example.test.LifeCycle Test = new org.example.test.LifeCycle();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@DisplayName("test_lifecycle_set")
	@Tag("Check")
	@Tag("KSAN")
	//@Tag("버킷의 Lifecycle 규칙을 추가 가능한지 확인")
	public void test_lifecycle_set()
    {
        Test.test_lifecycle_set();
	}

	@Test
	@DisplayName("test_lifecycle_get")
	@Tag("Get")
	@Tag("KSAN")
	//@Tag("버킷에 설정한 Lifecycle 규칙을 가져올 수 있는지 확인")
	public void test_lifecycle_get()
    {
        Test.test_lifecycle_get();
	}

	@Test
	@DisplayName("test_lifecycle_get_no_id")
	@Tag("Check")
	//@Tag("ID 없이 버킷에 Lifecycle 규칙을 설정 할 수 있는지 확인")
	public void test_lifecycle_get_no_id()
    {
        Test.test_lifecycle_get_no_id();
	}

	@Test
	@DisplayName("test_lifecycle_expiration_versioning_enabled")
    @Tag("Version")
    //@Tag("버킷에 버저닝 설정이 되어있는 상태에서 Lifecycle 규칙을 추가 가능한지 확인")
    public void test_lifecycle_expiration_versioning_enabled()
    {
        Test.test_lifecycle_expiration_versioning_enabled();
    }

	@Test
	@DisplayName("test_lifecycle_id_too_long")
    @Tag("Check")
    //@Tag("버킷에 Lifecycle 규칙을 설정할때 ID의 길이가 너무 길면 실패하는지 확인")
    public void test_lifecycle_id_too_long()
    {
        Test.test_lifecycle_id_too_long();
    }

	@Test
	@DisplayName("test_lifecycle_same_id")
    @Tag("Duplicate")
    //@Tag("버킷에 Lifecycle 규칙을 설정할때 같은 ID로 규칙을 여러개 설정할경우 실패하는지 확인")
    public void test_lifecycle_same_id()
    {
        Test.test_lifecycle_same_id();
    }

	@Test
	@DisplayName("test_lifecycle_invalid_status")
    @Tag("ERROR")
    //@Tag("버킷에 Lifecycle 규칙중 status를 잘못 설정할때 실패하는지 확인")
    public void test_lifecycle_invalid_status()
    {
        Test.test_lifecycle_invalid_status();
    }

	@Test
	@DisplayName("test_lifecycle_set_date")
    @Tag("Date")
    //@Tag("버킷의 Lifecycle규칙에 날짜를 입력가능한지 확인")
    public void test_lifecycle_set_date()
    {
        Test.test_lifecycle_set_date();
    }

	@Test
	@DisplayName("test_lifecycle_set_invalid_date")
    @Tag("ERROR")
    //@Tag("버킷의 Lifecycle규칙에 날짜를 올바르지 않은 형식으로 입력했을때 실패 확인")
    public void test_lifecycle_set_invalid_date()
    {
        Test.test_lifecycle_set_invalid_date();
    }

	@Test
	@DisplayName("test_lifecycle_set_noncurrent")
    @Tag("Version")
    //@Tag("버킷의 버저닝설정이 없는 환경에서 버전관리용 Lifecycle이 올바르게 설정되는지 확인")
    public void test_lifecycle_set_noncurrent()
    {
        Test.test_lifecycle_set_noncurrent();
    }

	@Test
	@DisplayName("test_lifecycle_noncur_expiration")
    @Tag("Version")
    //@Tag("버킷의 버저닝설정이 되어있는 환경에서 Lifecycle 이 올바르게 동작하는지 확인")
    public void test_lifecycle_noncur_expiration()
    {
        Test.test_lifecycle_noncur_expiration();
    }

	@Test
	@DisplayName("test_lifecycle_set_deletemarker")
    @Tag("DeleteMarker")
    //@Tag("DeleteMarker에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인")
    public void test_lifecycle_set_deletemarker()
    {
        Test.test_lifecycle_set_deletemarker();
    }

	@Test
	@DisplayName("test_lifecycle_set_filter")
    @Tag("Filter")
    //@Tag("Lifecycle 규칙에 필터링값을 설정 할 수 있는지 확인")
    public void test_lifecycle_set_filter()
    {
        Test.test_lifecycle_set_filter();
    }

	@Test
	@DisplayName("test_lifecycle_set_empty_filter")
    @Tag("Filter")
    //@Tag("Lifecycle 규칙에 필터링에 비어있는 값을 설정 할 수 있는지 확인")
    public void test_lifecycle_set_empty_filter()
    {
        Test.test_lifecycle_set_empty_filter();
    }

	@Test
	@DisplayName("test_lifecycle_deletemarker_expiration")
    @Tag("DeleteMarker")
    //@Tag("DeleteMarker에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인")
    public void test_lifecycle_deletemarker_expiration()
    {
        Test.test_lifecycle_deletemarker_expiration();
    }

	@Test
	@DisplayName("test_lifecycle_set_multipart")
    @Tag("Multipart")
    //@Tag("AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인")
    public void test_lifecycle_set_multipart()
    {
        Test.test_lifecycle_set_multipart();
        
    }

	@Test
	@DisplayName("test_lifecycle_multipart_expiration")
    @Tag("Multipart")
    //@Tag("AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인")
    public void test_lifecycle_multipart_expiration()
    {
        Test.test_lifecycle_multipart_expiration();
    }
	
	@Test
	@DisplayName("test_lifecycle_delete")
	@Tag("Delete")
	@Tag("KSAN")
	// @Tag("버킷의 Lifecycle 규칙을 삭제 가능한지 확인")
	public void test_lifecycle_delete()
	{
        Test.test_lifecycle_delete();
	}
}
