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
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class Policy {

	org.example.test.Policy test = new org.example.test.Policy();
	org.example.testV2.Policy testV2 = new org.example.testV2.Policy();

	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Check")
	// 버킷에 정책 설정이 올바르게 적용되는지 확인
	void testBucketPolicy() {
		test.testBucketPolicy();
		testV2.testBucketPolicy();
	}

	@Test
	@Tag("Check")
	// 버킷에 정책 설정이 올바르게 적용되는지 확인(ListObjectsV2)
	void testBucketV2Policy() {
		test.testBucketV2Policy();
		testV2.testBucketV2Policy();
	}

	@Test
	@Tag("Priority")
	// 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인
	void testBucketPolicyAcl() {
		test.testBucketPolicyAcl();
		testV2.testBucketPolicyAcl();
	}

	@Test
	@Tag("Priority")
	// 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인(ListObjectsV2)
	void testBucketV2PolicyAcl() {
		test.testBucketV2PolicyAcl();
		testV2.testBucketV2PolicyAcl();
	}

	@Test
	@Tag("Tagging")
	// 정책설정으로 오브젝트의 태그목록 읽기를 public-read로 설정했을때 올바르게 동작하는지 확인
	void testGetTagsAclPublic() {
		test.testGetTagsAclPublic();
		testV2.testGetTagsAclPublic();
	}

	@Test
	@Tag("Tagging")
	// 정책설정으로 오브젝트의 태그 입력을 public-read로 설정했을때 올바르게 동작하는지 확인
	void testPutTagsAclPublic() {
		test.testPutTagsAclPublic();
		testV2.testPutTagsAclPublic();
	}

	@Test
	@Tag("Tagging")
	// 정책설정으로 오브젝트의 태그 삭제를 public-read로 설정했을때 올바르게 동작하는지 확인
	void testDeleteTagsObjPublic() {
		test.testDeleteTagsObjPublic();
		testV2.testDeleteTagsObjPublic();
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObject허용] 조건부
	// 정책설정시 올바르게 동작하는지 확인
	void testBucketPolicyGetObjExistingTag() {
		test.testBucketPolicyGetObjExistingTag();
		testV2.testBucketPolicyGetObjExistingTag();
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게
	// GetObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
	void testBucketPolicyGetObjTaggingExistingTag() {
		test.testBucketPolicyGetObjTaggingExistingTag();
		testV2.testBucketPolicyGetObjTaggingExistingTag();
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게
	// PutObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
	void testBucketPolicyPutObjTaggingExistingTag() {
		test.testBucketPolicyPutObjTaggingExistingTag();
		testV2.testBucketPolicyPutObjTaggingExistingTag();
	}

	@Test
	@Tag("PathOptions")
	// [복사하려는 경로명이 'bucketName/public/*'에 해당할 경우에만 모든유저에게 PutObject허용] 조건부
	// 정책설정시 올바르게 동작하는지 확인
	void testBucketPolicyPutObjCopySource() {
		test.testBucketPolicyPutObjCopySource();
		testV2.testBucketPolicyPutObjCopySource();
	}

	@Test
	@Tag("MetadataOptions")
	// [오브젝트의 메타데이터값이 'x-amz-metadata-directive=COPY'일 경우에만 모든유저에게
	// PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	void testBucketPolicyPutObjCopySourceMeta() {
		test.testBucketPolicyPutObjCopySourceMeta();
		testV2.testBucketPolicyPutObjCopySourceMeta();
	}

	@Test
	@Tag("ACLOptions")
	// [PutObject는 모든유저에게 허용하지만 권한설정에 'public*'이 포함되면 업로드허용하지 않음] 조건부 정책설정시
	// 올바르게 동작하는지 확인
	void testBucketPolicyPutObjAcl() {
		test.testBucketPolicyPutObjAcl();
		testV2.testBucketPolicyPutObjAcl();
	}

	@Test
	@Tag("GrantOptions")
	// [오브젝트의 grant-full-control이 메인유저일 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시
	// 올바르게 동작하는지 확인
	void testBucketPolicyPutObjGrant() {
		test.testBucketPolicyPutObjGrant();
		testV2.testBucketPolicyPutObjGrant();
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectACL허용]
	// 조건부 정책설정시 올바르게 동작하는지 확인
	void testBucketPolicyGetObjAclExistingTag() {
		test.testBucketPolicyGetObjAclExistingTag();
		testV2.testBucketPolicyGetObjAclExistingTag();
	}

	@Test
	@Tag("Status")
	// [모든 사용자가 버킷에 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인
	void testGetPublicPolicyAclBucketPolicyStatus() {
		test.testGetPublicPolicyAclBucketPolicyStatus();
		testV2.testGetPublicPolicyAclBucketPolicyStatus();
	}

	@Test
	@Tag("Status")
	// [특정 ip로 접근했을때만 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인
	void testGetNonpublicPolicyAclBucketPolicyStatus() {
		test.testGetNonpublicPolicyAclBucketPolicyStatus();
		testV2.testGetNonpublicPolicyAclBucketPolicyStatus();
	}
}
