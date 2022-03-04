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

public class ListBuckets {

    org.example.test.ListBuckets Test = new org.example.test.ListBuckets();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}
    
    @Test
    @Tag("Get")
    @Tag("KSAN")
    // @Tag("여러개의 버킷 생성해서 목록 조회 확인")
    public void test_buckets_create_then_list()
    {
        Test.test_buckets_create_then_list();
    }

    @Test
    @Tag("ERROR")
    @Tag("KSAN")
    // @Tag("존재하지 않는 사용자가 버킷목록 조회시 에러 확인")
    public void test_list_buckets_invalid_auth()
    {
        Test.test_list_buckets_invalid_auth();
    }

    @Test
    @Tag("ERROR")
    @Tag("KSAN")
    // @Tag("로그인정보를 잘못입력한 사용자가 버킷목록 조회시 에러 확인")
    public void test_list_buckets_bad_auth()
    {
        Test.test_list_buckets_bad_auth();
    }

    @Test
    @Tag("Metadata")
    @Tag("KSAN")
    // Tag("버킷의 메타데이터를 가져올 수 있는지 확인")
    public void test_head_bucket()
    {
        Test.test_head_bucket();
    }
}
