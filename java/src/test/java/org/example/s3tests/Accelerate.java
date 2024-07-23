// package org.example.s3tests;

// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.TestInfo;
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.Tag;

// class Accelerate {
// 	org.example.test.Accelerate test = new org.example.test.Accelerate();
// 	org.example.testV2.Accelerate testV2 = new org.example.testV2.Accelerate();

// 	@AfterEach
// 	public void clear(TestInfo testInfo) {
// 		test.clear(testInfo);
// 		testV2.clear(testInfo);
// 	}

// 	@Test
// 	@Tag("Put")
// 	// 버킷 가속 설정이 가능한지 확인
// 	void testPutBucketAccelerate() {
// 		test.testPutBucketAccelerate();
// 		testV2.testPutBucketAccelerate();
// 	}

// 	@Test
// 	@Tag("Get")
// 	// 버킷 가속 설정이 올바르게 적용되는지 확인
// 	void testGetBucketAccelerate() {
// 		test.testGetBucketAccelerate();
// 		testV2.testGetBucketAccelerate();
// 	}

// 	@Test
// 	@Tag("Change")
// 	// 버킷 가속 설정이 변경되는지 확인
// 	void testChangeBucketAccelerate() {
// 		test.testChangeBucketAccelerate();
// 		testV2.testChangeBucketAccelerate();
// 	}

	
// 	@Test
// 	@Tag("Error")
// 	// 버킷 가속 설정을 잘못 입력했을 때 에러가 발생하는지 확인
// 	void testPutBucketAccelerateInvalid() {
// 		test.testPutBucketAccelerateInvalid();
// 		testV2.testPutBucketAccelerateInvalid();
// 	}
// }
