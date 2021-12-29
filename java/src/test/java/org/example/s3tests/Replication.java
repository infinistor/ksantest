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