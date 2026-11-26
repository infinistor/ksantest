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

/**
 * 버킷의 정책 기능을 테스트하는 클래스
 */
class Policy {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Policy Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Policy End");
	}


	org.example.test.Policy test = new org.example.test.Policy();
	org.example.testV2.Policy testV2 = new org.example.testV2.Policy();

	/**
	 * 테스트 정리 작업 수행
	 * 
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷에 정책 설정이 올바르게 적용되는지 확인
	 */
	@Test
	@Tag("Check")
	void testBucketPolicy() {
		test.testBucketPolicy();
		testV2.testBucketPolicy();
	}

	/**
	 * 버킷에 정책 설정이 올바르게 적용되는지 확인(ListObjectsV2)
	 */
	@Test
	@Tag("Check")
	void testBucketV2Policy() {
		test.testBucketV2Policy();
		testV2.testBucketV2Policy();
	}

	/**
	 * 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인
	 */
	@Test
	@Tag("Priority")
	void testBucketPolicyAcl() {
		test.testBucketPolicyAcl();
		testV2.testBucketPolicyAcl();
	}

	/**
	 * 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인(ListObjectsV2)
	 */
	@Test
	@Tag("Priority")
	void testBucketV2PolicyAcl() {
		test.testBucketV2PolicyAcl();
		testV2.testBucketV2PolicyAcl();
	}

	/**
	 * 정책설정으로 오브젝트의 태그목록 읽기를 public-read로 설정했을때 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("Tagging")
	void testGetTagsAclPublic() {
		test.testGetTagsAclPublic();
		testV2.testGetTagsAclPublic();
	}

	/**
	 * 정책설정으로 오브젝트의 태그 입력을 public-read로 설정했을때 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("Tagging")
	void testPutTagsAclPublic() {
		test.testPutTagsAclPublic();
		testV2.testPutTagsAclPublic();
	}

	/**
	 * 정책설정으로 오브젝트의 태그 삭제를 public-read로 설정했을때 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("Tagging")
	void testDeleteTagsObjPublic() {
		test.testDeleteTagsObjPublic();
		testV2.testDeleteTagsObjPublic();
	}

	/**
	 * [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObject허용]
	 * 조건부 정책설정시 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("TagOptions")
	void testBucketPolicyGetObjExistingTag() {
		test.testBucketPolicyGetObjExistingTag();
		testV2.testBucketPolicyGetObjExistingTag();
	}

	/**
	 * [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectTagging허용]
	 * 조건부 정책설정시 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("TagOptions")
	void testBucketPolicyGetObjTaggingExistingTag() {
		test.testBucketPolicyGetObjTaggingExistingTag();
		testV2.testBucketPolicyGetObjTaggingExistingTag();
	}

	/**
	 * [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 PutObjectTagging허용]
	 * 조건부 정책설정시 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("TagOptions")
	void testBucketPolicyPutObjTaggingExistingTag() {
		test.testBucketPolicyPutObjTaggingExistingTag();
		testV2.testBucketPolicyPutObjTaggingExistingTag();
	}

	/**
	 * [복사하려는 경로명이 'bucketName/public/*'에 해당할 경우에만 모든유저에게 PutObject허용]
	 * 조건부 정책설정시 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("PathOptions")
	void testBucketPolicyPutObjCopySource() {
		test.testBucketPolicyPutObjCopySource();
		testV2.testBucketPolicyPutObjCopySource();
	}

	/**
	 * [오브젝트의 메타데이터값이 'x-amz-metadata-directive=COPY'일 경우에만 모든유저에게 PutObject허용]
	 * 조건부 정책설정시 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("MetadataOptions")
	void testBucketPolicyPutObjCopySourceMeta() {
		test.testBucketPolicyPutObjCopySourceMeta();
		testV2.testBucketPolicyPutObjCopySourceMeta();
	}

	/**
	 * [PutObject는 모든유저에게 허용하지만 권한설정에 'public*'이 포함되면 업로드허용하지 않음]
	 * 조건부 정책설정시 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("ACLOptions")
	void testBucketPolicyPutObjAcl() {
		test.testBucketPolicyPutObjAcl();
		testV2.testBucketPolicyPutObjAcl();
	}

	/**
	 * [오브젝트의 grant-full-control이 메인유저일 경우에만 모든유저에게 PutObject허용]
	 * 조건부 정책설정시 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("GrantOptions")
	void testBucketPolicyPutObjGrant() {
		test.testBucketPolicyPutObjGrant();
		testV2.testBucketPolicyPutObjGrant();
	}

	/**
	 * [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectACL허용]
	 * 조건부 정책설정시 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("TagOptions")
	void testBucketPolicyGetObjAclExistingTag() {
		test.testBucketPolicyGetObjAclExistingTag();
		testV2.testBucketPolicyGetObjAclExistingTag();
	}

	/**
	 * 모든 사용자가 버킷에 접근 가능(public으으로 간주)
	 */
	@Test
	@Tag("Status")
	void testBucketPolicyStatusWithAllUser() {
		test.testBucketPolicyStatusWithAllUser();
		testV2.testBucketPolicyStatusWithAllUser();
	}

	/**
	 * 특정 사용자만 버킷에 접근 가능(private)
	 */
	@Test
	@Tag("Status")
	void testBucketPolicyStatusWithSpecificUserAccess() {
		test.testBucketPolicyStatusWithSpecificUserAccess();
		testV2.testBucketPolicyStatusWithSpecificUserAccess();
	}

	/**
	 * 너무 넓은 IP 범위를 가진 정책 (public으으로 간주)
	 */
	@Test
	@Tag("Status")
	void testBucketPolicyStatusWithWideIPRange() {
		test.testBucketPolicyStatusWithWideIPRange();
		testV2.testBucketPolicyStatusWithWideIPRange();
	}

	/**
	 * 특정 IP 범위를 가진 정책 (private)
	 */
	@Test
	@Tag("Status")
	void testBucketPolicyStatusWithIPRange() {
		test.testBucketPolicyStatusWithIPRange();
		testV2.testBucketPolicyStatusWithIPRange();
	}

	/**
	 * 매우 제한적인 시간에 대한 접근 허용 정책 (public으로 간주)
	 */
	@Test
	@Tag("Status")
	void testBucketPolicyStatusWithTimeCondition() {
		test.testBucketPolicyStatusWithTimeCondition();
		testV2.testBucketPolicyStatusWithTimeCondition();
	}

	/**
	 * 특정 태그를 가진 오브젝트에 대한 접근 허용용 정책 (public으로 간주)
	 */
	@Test
	@Tag("Status")
	void testBucketPolicyStatusWithTagCondition() {
		test.testBucketPolicyStatusWithTagCondition();
		testV2.testBucketPolicyStatusWithTagCondition();
	}

	/**
	 * 정책이 없는 버킷의 정책·정책 상태 조회가 NoSuchBucketPolicy 인지 확인
	 */
	@Test
	@Tag("Get")
	void testBucketPolicyGetNotExist() {
		test.testBucketPolicyGetNotExist();
		testV2.testBucketPolicyGetNotExist();
	}

	/**
	 * 정책이 없는 버킷의 정책 삭제가 성공하는지 확인
	 */
	@Test
	@Tag("Delete")
	void testBucketPolicyDeleteNotExist() {
		test.testBucketPolicyDeleteNotExist();
		testV2.testBucketPolicyDeleteNotExist();
	}

	/**
	 * 정책을 설정·조회·삭제하고 삭제 뒤에는 NoSuchBucketPolicy 인지 확인
	 */
	@Test
	@Tag("Check")
	void testBucketPolicyPutGetDelete() {
		test.testBucketPolicyPutGetDelete();
		testV2.testBucketPolicyPutGetDelete();
	}

	/**
	 * 없는 버킷의 정책 조회·설정·삭제가 NoSuchBucket 인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketPolicyBucketNotExist() {
		test.testBucketPolicyBucketNotExist();
		testV2.testBucketPolicyBucketNotExist();
	}

	/**
	 * 다른 사용자의 정책 조회·설정·삭제가 AccessDenied 이고 정책이 그대로인지 확인
	 */
	@Test
	@Tag("Access")
	void testBucketPolicyAccessDenied() {
		test.testBucketPolicyAccessDenied();
		testV2.testBucketPolicyAccessDenied();
	}

	/**
	 * JSON 이 아니거나 Statement 가 비었거나 Version 이 잘못된 정책이 MalformedPolicy 인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketPolicyInvalidJson() {
		test.testBucketPolicyInvalidJson();
		testV2.testBucketPolicyInvalidJson();
	}

	/**
	 * Effect·Action·Principal·Condition 이 올바르지 않은 정책이 MalformedPolicy 인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketPolicyInvalidElement() {
		test.testBucketPolicyInvalidElement();
		testV2.testBucketPolicyInvalidElement();
	}

	/**
	 * 다른 버킷 리소스, 액션과 맞지 않는 리소스, Resource·Principal 누락이 MalformedPolicy 인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketPolicyInvalidResource() {
		test.testBucketPolicyInvalidResource();
		testV2.testBucketPolicyInvalidResource();
	}

	/**
	 * 20KB 를 넘는 정책이 MalformedPolicy 인지 확인
	 */
	@Test
	@Tag("Max")
	void testBucketPolicyTooLarge() {
		test.testBucketPolicyTooLarge();
		testV2.testBucketPolicyTooLarge();
	}

	/**
	 * 허용한 사용자가 허용한 액션만 할 수 있고 나머지는 AccessDenied 인지 확인
	 */
	@Test
	@Tag("Effect")
	void testBucketPolicyAllowUser() {
		test.testBucketPolicyAllowUser();
		testV2.testBucketPolicyAllowUser();
	}

	/**
	 * Allow 와 Deny 가 겹치면 Deny 가 이기는지 확인
	 */
	@Test
	@Tag("Effect")
	void testBucketPolicyDenyOverridesAllow() {
		test.testBucketPolicyDenyOverridesAllow();
		testV2.testBucketPolicyDenyOverridesAllow();
	}

	/**
	 * 명시적 Deny 가 버킷 소유자에게도 적용되는지 확인
	 */
	@Test
	@Tag("Effect")
	void testBucketPolicyDenyOwner() {
		test.testBucketPolicyDenyOwner();
		testV2.testBucketPolicyDenyOwner();
	}

	/**
	 * Deny 의 NotAction 에 없는 액션만 거부되는지 확인
	 */
	@Test
	@Tag("NotAction")
	void testBucketPolicyNotActionDeny() {
		test.testBucketPolicyNotActionDeny();
		testV2.testBucketPolicyNotActionDeny();
	}

	/**
	 * Allow 의 NotAction 에 없는 액션만 허용되는지 확인
	 */
	@Test
	@Tag("NotAction")
	void testBucketPolicyNotActionAllow() {
		test.testBucketPolicyNotActionAllow();
		testV2.testBucketPolicyNotActionAllow();
	}

	/**
	 * Allow 의 NotResource 에 해당하지 않는 오브젝트만 허용되는지 확인
	 */
	@Test
	@Tag("NotResource")
	void testBucketPolicyNotResourceAllow() {
		test.testBucketPolicyNotResourceAllow();
		testV2.testBucketPolicyNotResourceAllow();
	}

	/**
	 * Deny 의 NotResource 에 해당하지 않는 오브젝트만 거부되는지 확인
	 */
	@Test
	@Tag("NotResource")
	void testBucketPolicyNotResourceDeny() {
		test.testBucketPolicyNotResourceDeny();
		testV2.testBucketPolicyNotResourceDeny();
	}

	/**
	 * NotPrincipal 은 Deny 에만 쓸 수 있고 그 주체만 거부를 피하는지 확인
	 */
	@Test
	@Tag("NotPrincipal")
	void testBucketPolicyNotPrincipal() {
		test.testBucketPolicyNotPrincipal();
		testV2.testBucketPolicyNotPrincipal();
	}

	/**
	 * Resource 의 *, ? 와일드카드와 대소문자 구분을 확인
	 */
	@Test
	@Tag("Wildcard")
	void testBucketPolicyResourceWildcard() {
		test.testBucketPolicyResourceWildcard();
		testV2.testBucketPolicyResourceWildcard();
	}

	/**
	 * Action 의 와일드카드(s3:Get*)가 해당 액션만 허용하는지 확인
	 */
	@Test
	@Tag("Wildcard")
	void testBucketPolicyActionWildcard() {
		test.testBucketPolicyActionWildcard();
		testV2.testBucketPolicyActionWildcard();
	}

	/**
	 * Action 이름이 대소문자를 구분하지 않는지 확인
	 */
	@Test
	@Tag("Wildcard")
	void testBucketPolicyActionCaseInsensitive() {
		test.testBucketPolicyActionCaseInsensitive();
		testV2.testBucketPolicyActionCaseInsensitive();
	}

	/**
	 * s3:ListBucket 만 허용하면 목록 조회만 되는지 확인
	 */
	@Test
	@Tag("Resource")
	void testBucketPolicyListBucketResource() {
		test.testBucketPolicyListBucketResource();
		testV2.testBucketPolicyListBucketResource();
	}

	/**
	 * s3:prefix 조건과 일치하는 목록 조회만 허용되는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicyListPrefixCondition() {
		test.testBucketPolicyListPrefixCondition();
		testV2.testBucketPolicyListPrefixCondition();
	}

	/**
	 * 조건 연산자가 여럿이면 모두 맞아야 허용되는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicyListConditionKeys() {
		test.testBucketPolicyListConditionKeys();
		testV2.testBucketPolicyListConditionKeys();
	}

	/**
	 * s3:prefix 조건의 Deny 가 요청의 prefix 값만 보는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicyListDenyPrefix() {
		test.testBucketPolicyListDenyPrefix();
		testV2.testBucketPolicyListDenyPrefix();
	}

	/**
	 * s3:max-keys 조건이 요청 값으로 판정되는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicyListMaxKeys() {
		test.testBucketPolicyListMaxKeys();
		testV2.testBucketPolicyListMaxKeys();
	}

	/**
	 * aws:SecureTransport 조건으로 http 요청만 거부되는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicySecureTransport() {
		test.testBucketPolicySecureTransport();
		testV2.testBucketPolicySecureTransport();
	}

	/**
	 * aws:SourceIp 조건(IpAddress, NotIpAddress)이 올바르게 판정되는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicySourceIp() {
		test.testBucketPolicySourceIp();
		testV2.testBucketPolicySourceIp();
	}

	/**
	 * 암호화 헤더가 AES256 이 아닌 업로드·멀티파트·복사가 거부되는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicyDenyUnencryptedPut() {
		test.testBucketPolicyDenyUnencryptedPut();
		testV2.testBucketPolicyDenyUnencryptedPut();
	}

	/**
	 * Null 조건으로 암호화 헤더가 없는 업로드가 거부되는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicyNullCondition() {
		test.testBucketPolicyNullCondition();
		testV2.testBucketPolicyNullCondition();
	}

	/**
	 * 버전을 지정한 조회는 s3:GetObjectVersion 이 필요한지 확인
	 */
	@Test
	@Tag("Version")
	void testBucketPolicyGetObjectVersionAction() {
		test.testBucketPolicyGetObjectVersionAction();
		testV2.testBucketPolicyGetObjectVersionAction();
	}

	/**
	 * s3:VersionId 조건과 일치하는 버전만 조회되는지 확인
	 */
	@Test
	@Tag("Version")
	void testBucketPolicyVersionIdCondition() {
		test.testBucketPolicyVersionIdCondition();
		testV2.testBucketPolicyVersionIdCondition();
	}

	/**
	 * 버전을 지정한 삭제는 s3:DeleteObjectVersion 이 필요한지 확인
	 */
	@Test
	@Tag("Version")
	void testBucketPolicyDeleteObjectVersionAction() {
		test.testBucketPolicyDeleteObjectVersionAction();
		testV2.testBucketPolicyDeleteObjectVersionAction();
	}

	/**
	 * s3:RequestObjectTagKeys 조건으로 허용한 태그 키만 올릴 수 있는지 확인
	 */
	@Test
	@Tag("Condition")
	void testBucketPolicyRequestObjectTagKeys() {
		test.testBucketPolicyRequestObjectTagKeys();
		testV2.testBucketPolicyRequestObjectTagKeys();
	}

	/**
	 * s3:PutObject 가 허용하는 범위(업로드·멀티파트)와 허용하지 않는 범위를 확인
	 */
	@Test
	@Tag("Action")
	void testBucketPolicyPutObjectAction() {
		test.testBucketPolicyPutObjectAction();
		testV2.testBucketPolicyPutObjectAction();
	}

	/**
	 * 멀티파트 목록 액션만 허용하면 목록 조회만 되는지 확인
	 */
	@Test
	@Tag("Action")
	void testBucketPolicyMultipartListActions() {
		test.testBucketPolicyMultipartListActions();
		testV2.testBucketPolicyMultipartListActions();
	}

	/**
	 * 버킷 설정 액션을 허용한 만큼만 조회·설정·삭제되는지 확인
	 */
	@Test
	@Tag("Action")
	void testBucketPolicyBucketConfigActions() {
		test.testBucketPolicyBucketConfigActions();
		testV2.testBucketPolicyBucketConfigActions();
	}

	/**
	 * Statement 단일 객체, Version 생략, Id·Sid, 여러 주체 형식을 받는지 확인
	 */
	@Test
	@Tag("Form")
	void testBucketPolicyStatementForms() {
		test.testBucketPolicyStatementForms();
		testV2.testBucketPolicyStatementForms();
	}

	/**
	 * 형식이 올바르지 않은 문장이 MalformedPolicy 인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketPolicyInvalidStatement() {
		test.testBucketPolicyInvalidStatement();
		testV2.testBucketPolicyInvalidStatement();
	}

	/**
	 * 정책을 삭제하면 허용했던 권한이 사라지는지 확인
	 */
	@Test
	@Tag("Delete")
	void testBucketPolicyDeleteRevokes() {
		test.testBucketPolicyDeleteRevokes();
		testV2.testBucketPolicyDeleteRevokes();
	}

	/**
	 * 다른 사용자가 정책으로 올린 오브젝트를 소유자와 그 사용자가 읽고 지울 수 있는지 확인
	 */
	@Test
	@Tag("Effect")
	void testBucketPolicyOtherUserWrite() {
		test.testBucketPolicyOtherUserWrite();
		testV2.testBucketPolicyOtherUserWrite();
	}

	/**
	 * 익명 사용자에게 허용한 접두어의 오브젝트만 읽을 수 있는지 확인
	 */
	@Test
	@Tag("Effect")
	void testBucketPolicyAnonymousPrefix() {
		test.testBucketPolicyAnonymousPrefix();
		testV2.testBucketPolicyAnonymousPrefix();
	}
}
