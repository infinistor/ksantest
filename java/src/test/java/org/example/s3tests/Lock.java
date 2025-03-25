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
 * 버킷과 오브젝트의 잠금 기능을 테스트하는 클래스
 */
class Lock {

	org.example.test.Lock test = new org.example.test.Lock();
	org.example.testV2.Lock testV2 = new org.example.testV2.Lock();

	/**
	 * 테스트 정리 작업 수행
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷을 생성한 후 오브젝트의 잠금 설정을 활성화 할 수 있는지 확인
	 */
	@Test
	@Tag("Put")
	void testCreatedBucketEnableObjectLock() {
		test.testCreatedBucketEnableObjectLock();
		testV2.testCreatedBucketEnableObjectLock();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 잠금 설정이 가능한지 확인
	 */
	@Test
	@Tag("Check")
	void testObjectLockPutObjLock() {
		test.testObjectLockPutObjLock();
		testV2.testObjectLockPutObjLock();
	}

	/**
	 * 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정이 실패
	 */
	@Test
	@Tag("ERROR")
	void testObjectLockPutObjLockInvalidBucket() {
		test.testObjectLockPutObjLockInvalidBucket();
		testV2.testObjectLockPutObjLockInvalidBucket();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패
	 */
	@Test
	@Tag("ERROR")
	void testObjectLockPutObjLockWithDaysAndYears() {
		test.testObjectLockPutObjLockWithDaysAndYears();
		testV2.testObjectLockPutObjLockWithDaysAndYears();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패
	 */
	@Test
	@Tag("ERROR")
	void testObjectLockPutObjLockInvalidDays() {
		test.testObjectLockPutObjLockInvalidDays();
		testV2.testObjectLockPutObjLockInvalidDays();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패
	 */
	@Test
	@Tag("ERROR")
	void testObjectLockPutObjLockInvalidYears() {
		test.testObjectLockPutObjLockInvalidYears();
		testV2.testObjectLockPutObjLockInvalidYears();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	 */
	@Test
	@Tag("ERROR")
	void testObjectLockPutObjLockInvalidMode() {
		test.testObjectLockPutObjLockInvalidMode();
		testV2.testObjectLockPutObjLockInvalidMode();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	 */
	@Test
	@Tag("ERROR")
	void testObjectLockPutObjLockInvalidStatus() {
		test.testObjectLockPutObjLockInvalidStatus();
		testV2.testObjectLockPutObjLockInvalidStatus();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패
	 */
	@Test
	@Tag("Version")
	void testObjectLockSuspendVersioning() {
		test.testObjectLockSuspendVersioning();
		testV2.testObjectLockSuspendVersioning();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인
	 */
	@Test
	@Tag("Check")
	void testObjectLockGetObjLock() {
		test.testObjectLockGetObjLock();
		testV2.testObjectLockGetObjLock();
	}

	/**
	 * 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패
	 */
	@Test
	@Tag("ERROR")
	void testObjectLockGetObjLockInvalidBucket() {
		test.testObjectLockGetObjLockInvalidBucket();
		testV2.testObjectLockGetObjLockInvalidBucket();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인
	 */
	@Test
	@Tag("Retention")
	void testObjectLockPutObjRetention() {
		test.testObjectLockPutObjRetention();
		testV2.testObjectLockPutObjRetention();
	}

	/**
	 * 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패
	 */
	@Test
	@Tag("Retention")
	void testObjectLockPutObjRetentionInvalidBucket() {
		test.testObjectLockPutObjRetentionInvalidBucket();
		testV2.testObjectLockPutObjRetentionInvalidBucket();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패
	 */
	@Test
	@Tag("Retention")
	void testObjectLockPutObjRetentionInvalidMode() {
		test.testObjectLockPutObjRetentionInvalidMode();
		testV2.testObjectLockPutObjRetentionInvalidMode();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인
	 */
	@Test
	@Tag("Retention")
	void testObjectLockGetObjRetention() {
		test.testObjectLockGetObjRetention();
		testV2.testObjectLockGetObjRetention();
	}

	/**
	 * 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패
	 */
	@Test
	@Tag("Retention")
	void testObjectLockGetObjRetentionInvalidBucket() {
		test.testObjectLockGetObjRetentionInvalidBucket();
		testV2.testObjectLockGetObjRetentionInvalidBucket();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인
	 */
	@Test
	@Tag("Retention")
	void testObjectLockPutObjRetentionVersionid() {
		test.testObjectLockPutObjRetentionVersionid();
		testV2.testObjectLockPutObjRetentionVersionid();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인
	 */
	@Test
	@Tag("Priority")
	void testObjectLockPutObjRetentionOverrideDefaultRetention() {
		test.testObjectLockPutObjRetentionOverrideDefaultRetention();
		testV2.testObjectLockPutObjRetentionOverrideDefaultRetention();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인
	 */
	@Test
	@Tag("Overwrite")
	void testObjectLockPutObjRetentionIncreasePeriod() {
		test.testObjectLockPutObjRetentionIncreasePeriod();
		testV2.testObjectLockPutObjRetentionIncreasePeriod();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인
	 */
	@Test
	@Tag("Overwrite")
	void testObjectLockPutObjRetentionShortenPeriod() {
		test.testObjectLockPutObjRetentionShortenPeriod();
		testV2.testObjectLockPutObjRetentionShortenPeriod();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인
	 */
	@Test
	@Tag("Overwrite")
	void testObjectLockPutObjRetentionShortenPeriodBypass() {
		test.testObjectLockPutObjRetentionShortenPeriodBypass();
		testV2.testObjectLockPutObjRetentionShortenPeriodBypass();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testObjectLockDeleteObjectWithRetention() {
		test.testObjectLockDeleteObjectWithRetention();
		testV2.testObjectLockDeleteObjectWithRetention();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인
	 */
	@Test
	@Tag("LegalHold")
	void testObjectLockPutLegalHold() {
		test.testObjectLockPutLegalHold();
		testV2.testObjectLockPutLegalHold();
	}

	/**
	 * [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인
	 */
	@Test
	@Tag("LegalHold")
	void testObjectLockPutLegalHoldInvalidBucket() {
		test.testObjectLockPutLegalHoldInvalidBucket();
		testV2.testObjectLockPutLegalHoldInvalidBucket();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold에 잘못된 값을 넣을 경우 실패 확인
	 */
	@Test
	@Tag("LegalHold")
	void testObjectLockPutLegalHoldInvalidStatus() {
		test.testObjectLockPutLegalHoldInvalidStatus();
		testV2.testObjectLockPutLegalHoldInvalidStatus();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인
	 */
	@Test
	@Tag("LegalHold")
	void testObjectLockGetLegalHold() {
		test.testObjectLockGetLegalHold();
		testV2.testObjectLockGetLegalHold();
	}

	/**
	 * [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold설정 조회 실패 확인
	 */
	@Test
	@Tag("LegalHold")
	void testObjectLockGetLegalHoldInvalidBucket() {
		test.testObjectLockGetLegalHoldInvalidBucket();
		testV2.testObjectLockGetLegalHoldInvalidBucket();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인
	 */
	@Test
	@Tag("LegalHold")
	void testObjectLockDeleteObjectWithLegalHoldOn() {
		test.testObjectLockDeleteObjectWithLegalHoldOn();
		testV2.testObjectLockDeleteObjectWithLegalHoldOn();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인
	 */
	@Test
	@Tag("LegalHold")
	void testObjectLockDeleteObjectWithLegalHoldOff() {
		test.testObjectLockDeleteObjectWithLegalHoldOff();
		testV2.testObjectLockDeleteObjectWithLegalHoldOff();
	}

	/**
	 * [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인
	 */
	@Test
	@Tag("LegalHold")
	void testObjectLockGetObjMetadata() {
		test.testObjectLockGetObjMetadata();
		testV2.testObjectLockGetObjMetadata();
	}

}
