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

public class Policy {

	org.example.test.Policy Test = new org.example.test.Policy();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("Check")
	// @Tag("버킷에 정책 설정이 올바르게 적용되는지 확인
	public void test_bucket_policy() {
		Test.test_bucket_policy();
	}

	@Test
	@Tag("Check")
	// @Tag("버킷에 정책 설정이 올바르게 적용되는지 확인(ListObjectsV2)
	public void test_bucketv2_policy() {
		Test.test_bucketv2_policy();
	}

	@Test
	@Tag("Priority")
	// @Tag("버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인
	public void test_bucket_policy_acl() {
		Test.test_bucket_policy_acl();
	}

	@Test
	@Tag("Priority")
	// @Tag("버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인(ListObjectsV2)
	public void test_bucketv2_policy_acl() {
		Test.test_bucketv2_policy_acl();
	}

	@Test
	@Tag("Taggings")
	// @Tag("정책설정으로 오브젝트의 태그목록 읽기를 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_get_tags_acl_public() {
		Test.test_get_tags_acl_public();
	}

	@Test
	@Tag("Tagging")
	// @Tag("정책설정으로 오브젝트의 태그 입력을 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_put_tags_acl_public() {
		Test.test_put_tags_acl_public();
	}

	@Test
	@Tag("Tagging")
	// @Tag("정책설정으로 오브젝트의 태그 삭제를 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_delete_tags_obj_public() {
		Test.test_delete_tags_obj_public();
	}

	@Test
	@Tag("TagOptions")
	// @Tag("[오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObject허용] 조건부
	// 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_existing_tag() {
		Test.test_bucket_policy_get_obj_existing_tag();
	}

	@Test
	@Tag("TagOptions")
	// @Tag("[오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게
	// GetObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_tagging_existing_tag() {
		Test.test_bucket_policy_get_obj_tagging_existing_tag();
	}

	@Test
	@Tag("TagOptions")
	// @Tag("[오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게
	// PutObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_tagging_existing_tag() {
		Test.test_bucket_policy_put_obj_tagging_existing_tag();
	}

	@Test
	@Tag("PathOptions")
	// @Tag("[복사하려는 경로명이 'BucketName/public/*'에 해당할 경우에만 모든유저에게 PutObject허용] 조건부
	// 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_copy_source() {
		Test.test_bucket_policy_put_obj_copy_source();
	}

	@Test
	@Tag("MetadataOptions")
	// @Tag("[오브젝트의 메타데이터값이 'x-amz-metadata-directive=COPY'일 경우에만 모든유저에게
	// PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_copy_source_meta() {
		Test.test_bucket_policy_put_obj_copy_source_meta();
	}

	@Test
	@Tag("ACLOptions")
	// @Tag("[PutObject는 모든유저에게 허용하지만 권한설정에 'public*'이 포함되면 업로드허용하지 않음] 조건부 정책설정시
	// 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_acl() {
		Test.test_bucket_policy_put_obj_acl();
	}

	@Test
	@Tag("GrantOptions")
	// @Tag("[오브젝트의 grant-full-control이 메인유저일 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시
	// 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_grant() {
		Test.test_bucket_policy_put_obj_grant();
	}

	@Test
	@Tag("TagOptions")
	// @Tag("[오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectACL허용]
	// 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_acl_existing_tag() {
		Test.test_bucket_policy_get_obj_acl_existing_tag();
	}

	@Test
	@Tag("Status")
	// @Tag("[모든 사용자가 버킷에 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인
	public void test_get_publicpolicy_acl_bucket_policy_status() {
		Test.test_get_publicpolicy_acl_bucket_policy_status();
	}

	@Test
	@Tag("Status")
	// @Tag("[특정 ip로 접근했을때만 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인
	public void test_get_nonpublicpolicy_acl_bucket_policy_status() {
		Test.test_get_nonpublicpolicy_acl_bucket_policy_status();
	}
}
