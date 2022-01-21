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

public class Replication {

    org.example.test.Replication Test = new org.example.test.Replication();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @DisplayName("test_replication_set")
    @Tag("Check")
    // @Tag("버킷의 Replication 설정이 되는지 확인(put/get/delete)")
    public void test_replication_set()
    {
        Test.test_replication_set();
    }

    @Test
    @DisplayName("test_replication_no_rule")
    @Tag("Check")
    // @Tag("복제설정중 role이 없어도 설정되는지 확인")
    public void test_replication_no_rule()
    {
        Test.test_replication_no_rule();
    }

    @Test
    @DisplayName("test_replication_full_copy")
    @Tag("Check")
    // @Tag("버킷의 복제설정이 올바르게 동작하는지 확인")
    public void test_replication_full_copy()
    {
        Test.test_replication_full_copy();
    }

    @Test
    @DisplayName("test_replication_tagging")
    @Tag("Check")
    // @Tag("버킷에 복제 설정이 되어 있을때 태그가 복제되는지 확인")
    public void test_replication_tagging()
    {
        Test.test_replication_tagging();
    }

    @Test
    @DisplayName("test_replication_prefix_copy")
    @Tag("Check")
    // @Tag("버킷의 복제 설정중 prefix가 올바르게 동작하는지 확인")
    public void test_replication_prefix_copy()
    {
        Test.test_replication_prefix_copy();
    }

    @Test
    @DisplayName("test_replication_deletemarker_copy")
    @Tag("Check")
    // @Tag("버킷의 복제 설정중 DeleteMarker가 올바르게 동작하는지 확인")
    public void test_replication_deletemarker_copy()
    {
        Test.test_replication_deletemarker_copy();
    }

    @Test
    @DisplayName("trest_replication_invalid_source_bucket_name")
    @Tag("ERROR")
    // @Tag("원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")
    public void trest_replication_invalid_source_bucket_name()
    {
        Test.trest_replication_invalid_source_bucket_name();
    }

    @Test
    @DisplayName("trest_replication_invalid_source_bucket_versioning")
    @Tag("ERROR")
    // @Tag("원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")
    public void trest_replication_invalid_source_bucket_versioning()
    {
        Test.trest_replication_invalid_source_bucket_versioning();
    }

    @Test
    @DisplayName("trest_replication_invalid_target_bucket_name")
    @Tag("ERROR")
    // @Tag("대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")
    public void trest_replication_invalid_target_bucket_name()
    {
        Test.trest_replication_invalid_target_bucket_name();
    }

    @Test
    @DisplayName("trest_replication_invalid_target_bucket_versioning")
    @Tag("ERROR")
    // @Tag("대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")
    public void trest_replication_invalid_target_bucket_versioning()
    {
        Test.trest_replication_invalid_target_bucket_versioning();
    }

}