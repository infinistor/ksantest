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

class Lock {

	org.example.test.Lock Test = new org.example.test.Lock();

	@AfterEach
	public void Clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("Check")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 잠금 설정이 가능한지 확인
	void testObjectLockPutObjLock() {
		Test.testObjectLockPutObjLock();
	}

	@Test
	@Tag("ERROR")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정이 실패
	void testObjectLockPutObjLockInvalidBucket() {
		Test.testObjectLockPutObjLockInvalidBucket();
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패
	void testObjectLockPutObjLockWithDaysAndYears() {
		Test.testObjectLockPutObjLockWithDaysAndYears();
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패
	void testObjectLockPutObjLockInvalidDays() {
		Test.testObjectLockPutObjLockInvalidDays();
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패
	void testObjectLockPutObjLockInvalidYears() {
		Test.testObjectLockPutObjLockInvalidYears();
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	void testObjectLockPutObjLockInvalidMode() {
		Test.testObjectLockPutObjLockInvalidMode();
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	void testObjectLockPutObjLockInvalidStatus() {
		Test.testObjectLockPutObjLockInvalidStatus();
	}

	@Test
	@Tag("Version")
	// [버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패
	void testObjectLockSuspendVersioning() {
		Test.testObjectLockSuspendVersioning();
	}

	@Test
	@Tag("Check")
	// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인
	void testObjectLockGetObjLock() {
		Test.testObjectLockGetObjLock();
	}

	@Test
	@Tag("ERROR")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패
	void testObjectLockGetObjLockInvalidBucket() {
		Test.testObjectLockGetObjLockInvalidBucket();
	}

	@Test
	@Tag("Retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인
	void testObjectLockPutObjRetention() {
		Test.testObjectLockPutObjRetention();
	}

	@Test
	@Tag("Retention")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패
	void testObjectLockPutObjRetentionInvalidBucket() {
		Test.testObjectLockPutObjRetentionInvalidBucket();
	}

	@Test
	@Tag("Retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패
	void testObjectLockPutObjRetentionInvalidMode() {
		Test.testObjectLockPutObjRetentionInvalidMode();
	}

	@Test
	@Tag("Retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인
	void testObjectLockGetObjRetention() {
		Test.testObjectLockGetObjRetention();
	}

	@Test
	@Tag("Retention")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패
	void testObjectLockGetObjRetentionInvalidBucket() {
		Test.testObjectLockGetObjRetentionInvalidBucket();
	}

	@Test
	@Tag("Retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인
	void testObjectLockPutObjRetentionVersionid() {
		Test.testObjectLockPutObjRetentionVersionid();
	}

	@Test
	@Tag("Priority")
	// [버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인
	void testObjectLockPutObjRetentionOverrideDefaultRetention() {
		Test.testObjectLockPutObjRetentionOverrideDefaultRetention();
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인
	void testObjectLockPutObjRetentionIncreasePeriod() {
		Test.testObjectLockPutObjRetentionIncreasePeriod();
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인
	void testObjectLockPutObjRetentionShortenPeriod() {
		Test.testObjectLockPutObjRetentionShortenPeriod();
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인
	void testObjectLockPutObjRetentionShortenPeriodBypass() {
		Test.testObjectLockPutObjRetentionShortenPeriodBypass();
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인
	void testObjectLockDeleteObjectWithRetention() {
		Test.testObjectLockDeleteObjectWithRetention();
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인
	void testObjectLockPutLegalHold() {
		Test.testObjectLockPutLegalHold();
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인
	void testObjectLockPutLegalHoldInvalidBucket() {
		Test.testObjectLockPutLegalHoldInvalidBucket();
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold에 잘못된 값을 넣을 경우 실패 확인
	void testObjectLockPutLegalHoldInvalidStatus() {
		Test.testObjectLockPutLegalHoldInvalidStatus();
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인
	void testObjectLockGetLegalHold() {
		Test.testObjectLockGetLegalHold();
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold설정 조회 실패 확인
	void testObjectLockGetLegalHoldInvalidBucket() {
		Test.testObjectLockGetLegalHoldInvalidBucket();
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인
	void testObjectLockDeleteObjectWithLegalHoldOn() {
		Test.testObjectLockDeleteObjectWithLegalHoldOn();
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인
	void testObjectLockDeleteObjectWithLegalHoldOff() {
		Test.testObjectLockDeleteObjectWithLegalHoldOff();
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인
	void testObjectLockGetObjMetadata() {
		Test.testObjectLockGetObjMetadata();
	}

}
