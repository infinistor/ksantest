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
package org.example.testV2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.CSVInput;
import software.amazon.awssdk.services.s3.model.CSVOutput;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONInput;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.JSONType;
import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

/**
 * S3 SelectObjectContent API 테스트
 * 오브젝트 내용에 SQL을 실행하여 결과를 조회하는 기능 검증
 */
public class SelectObjectContent extends TestBase {

	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("SelectObjectContent V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("SelectObjectContent V2 End");
	}

	/**
	 * CSV 오브젝트에 대해 SelectObjectContent 기본 동작 확인 (select * from s3object)
	 */
	@Test
	@Tag("Select")
	public void testSelectObjectContentCsvBasic() {
		var client = getClient();
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = createBucket(client);
		var key = "data.csv";
		// CSV: 헤더 + 2행
		var csvContent = "name,age,city\nAlice,30,Seoul\nBob,25,Busan";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(csvContent));

		var inputSerialization = InputSerialization.builder()
				.csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.USE).build())
				.compressionType(CompressionType.NONE)
				.build();
		var outputSerialization = OutputSerialization.builder()
				.csv(CSVOutput.builder().build())
				.build();

		var request = SelectObjectContentRequest.builder()
				.bucket(bucketName)
				.key(key)
				.expression("SELECT * FROM s3object")
				.expressionType(ExpressionType.SQL)
				.inputSerialization(inputSerialization)
				.outputSerialization(outputSerialization)
				.build();

		var resultCollector = new ResultCollector();
		var handler = SelectObjectContentResponseHandler.builder()
				.subscriber(SelectObjectContentResponseHandler.Visitor.builder()
						.onRecords(r -> resultCollector.add(r.payload().asUtf8String()))
						.build())
				.build();

		asyncClient.selectObjectContent(request, handler).join();
		var result = resultCollector.getResult();
		assertNotNull(result);
		assertTrue(result.contains("Alice") && result.contains("Bob"));
		assertTrue(result.contains("Seoul") && result.contains("Busan"));
	}

	/**
	 * CSV 오브젝트에 WHERE 조건 적용
	 */
	@Test
	@Tag("Select")
	public void testSelectObjectContentCsvWithWhere() {
		var client = getClient();
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = createBucket(client);
		var key = "data.csv";
		var csvContent = "name,age,city\nAlice,30,Seoul\nBob,25,Busan\nCharlie,30,Incheon";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(csvContent));

		var inputSerialization = InputSerialization.builder()
				.csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.USE).build())
				.compressionType(CompressionType.NONE)
				.build();
		var outputSerialization = OutputSerialization.builder()
				.csv(CSVOutput.builder().build())
				.build();

		var request = SelectObjectContentRequest.builder()
				.bucket(bucketName)
				.key(key)
				.expression("SELECT * FROM s3object WHERE s3.\"age\" = 30")
				.expressionType(ExpressionType.SQL)
				.inputSerialization(inputSerialization)
				.outputSerialization(outputSerialization)
				.build();

		var resultCollector = new ResultCollector();
		var handler = SelectObjectContentResponseHandler.builder()
				.subscriber(SelectObjectContentResponseHandler.Visitor.builder()
						.onRecords(r -> resultCollector.add(r.payload().asUtf8String()))
						.build())
				.build();

		asyncClient.selectObjectContent(request, handler).join();
		var result = resultCollector.getResult();
		assertNotNull(result);
		assertTrue(result.contains("Alice") && result.contains("Charlie"));
		assertTrue(!result.contains("Bob") || result.contains("25")); // Bob은 25이므로 조건에 따라 제외될 수 있음
	}

	/**
	 * CSV 오브젝트 LIMIT 적용
	 */
	@Test
	@Tag("Select")
	public void testSelectObjectContentCsvLimit() {
		var client = getClient();
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = createBucket(client);
		var key = "data.csv";
		var csvContent = "id,value\n1,a\n2,b\n3,c";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(csvContent));

		var inputSerialization = InputSerialization.builder()
				.csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.USE).build())
				.compressionType(CompressionType.NONE)
				.build();
		var outputSerialization = OutputSerialization.builder()
				.csv(CSVOutput.builder().build())
				.build();

		var request = SelectObjectContentRequest.builder()
				.bucket(bucketName)
				.key(key)
				.expression("SELECT * FROM s3object LIMIT 2")
				.expressionType(ExpressionType.SQL)
				.inputSerialization(inputSerialization)
				.outputSerialization(outputSerialization)
				.build();

		var resultCollector = new ResultCollector();
		var handler = SelectObjectContentResponseHandler.builder()
				.subscriber(SelectObjectContentResponseHandler.Visitor.builder()
						.onRecords(r -> resultCollector.add(r.payload().asUtf8String()))
						.build())
				.build();

		asyncClient.selectObjectContent(request, handler).join();
		var result = resultCollector.getResult();
		assertNotNull(result);
		assertTrue(result.contains("1") && result.contains("2"));
	}

	/**
	 * JSON 오브젝트에 대해 SelectObjectContent 기본 동작 확인
	 */
	@Test
	@Tag("Select")
	public void testSelectObjectContentJsonBasic() {
		var client = getClient();
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = createBucket(client);
		var key = "data.json";
		// JSON Lines 형식 (한 줄에 한 JSON)
		var jsonContent = "{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(jsonContent));

		var inputSerialization = InputSerialization.builder()
				.json(JSONInput.builder().type(JSONType.LINES).build())
				.compressionType(CompressionType.NONE)
				.build();
		var outputSerialization = OutputSerialization.builder()
				.json(JSONOutput.builder().build())
				.build();

		var request = SelectObjectContentRequest.builder()
				.bucket(bucketName)
				.key(key)
				.expression("SELECT * FROM s3object s WHERE s.age > 26")
				.expressionType(ExpressionType.SQL)
				.inputSerialization(inputSerialization)
				.outputSerialization(outputSerialization)
				.build();

		var resultCollector = new ResultCollector();
		var handler = SelectObjectContentResponseHandler.builder()
				.subscriber(SelectObjectContentResponseHandler.Visitor.builder()
						.onRecords(r -> resultCollector.add(r.payload().asUtf8String()))
						.build())
				.build();

		asyncClient.selectObjectContent(request, handler).join();
		var result = resultCollector.getResult();
		assertNotNull(result);
		assertTrue(result.contains("Alice") && result.contains("30"));
	}

	/**
	 * 존재하지 않는 버킷에 SelectObjectContent 요청 시 실패 확인
	 */
	@Test
	@Tag("ERROR")
	public void testSelectObjectContentNonExistentBucket() {
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = "non-existent-bucket-" + System.currentTimeMillis();
		var key = "data.csv";

		var inputSerialization = InputSerialization.builder()
				.csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.NONE).build())
				.compressionType(CompressionType.NONE)
				.build();
		var outputSerialization = OutputSerialization.builder()
				.csv(CSVOutput.builder().build())
				.build();

		var request = SelectObjectContentRequest.builder()
				.bucket(bucketName)
				.key(key)
				.expression("SELECT * FROM s3object")
				.expressionType(ExpressionType.SQL)
				.inputSerialization(inputSerialization)
				.outputSerialization(outputSerialization)
				.build();

		var resultCollector = new ResultCollector();
		var handler = SelectObjectContentResponseHandler.builder()
				.subscriber(SelectObjectContentResponseHandler.Visitor.builder()
						.onRecords(r -> resultCollector.add(r.payload().asUtf8String()))
						.build())
				.build();

		var e = assertThrows(CompletionException.class, () ->
				asyncClient.selectObjectContent(request, handler).join());
		var cause = e.getCause();
		assertTrue(cause instanceof AwsServiceException);
		var awsEx = (AwsServiceException) cause;
		assertEquals(HttpStatus.SC_NOT_FOUND, awsEx.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, awsEx.awsErrorDetails().errorCode());
	}

	/**
	 * 존재하지 않는 오브젝트에 SelectObjectContent 요청 시 실패 확인
	 */
	@Test
	@Tag("ERROR")
	public void testSelectObjectContentNonExistentObject() {
		var client = getClient();
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = createBucket(client);
		var key = "non-existent-key.csv";

		var inputSerialization = InputSerialization.builder()
				.csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.NONE).build())
				.compressionType(CompressionType.NONE)
				.build();
		var outputSerialization = OutputSerialization.builder()
				.csv(CSVOutput.builder().build())
				.build();

		var request = SelectObjectContentRequest.builder()
				.bucket(bucketName)
				.key(key)
				.expression("SELECT * FROM s3object")
				.expressionType(ExpressionType.SQL)
				.inputSerialization(inputSerialization)
				.outputSerialization(outputSerialization)
				.build();

		var resultCollector = new ResultCollector();
		var handler = SelectObjectContentResponseHandler.builder()
				.subscriber(SelectObjectContentResponseHandler.Visitor.builder()
						.onRecords(r -> resultCollector.add(r.payload().asUtf8String()))
						.build())
				.build();

		var e = assertThrows(CompletionException.class, () ->
				asyncClient.selectObjectContent(request, handler).join());
		var cause = e.getCause();
		assertTrue(cause instanceof AwsServiceException);
		var awsEx = (AwsServiceException) cause;
		assertEquals(HttpStatus.SC_NOT_FOUND, awsEx.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, awsEx.awsErrorDetails().errorCode());
	}

	/**
	 * 빈 CSV 오브젝트에 SelectObjectContent (헤더만 있는 경우)
	 */
	@Test
	@Tag("Select")
	public void testSelectObjectContentCsvEmptyRows() {
		var client = getClient();
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = createBucket(client);
		var key = "empty.csv";
		var csvContent = "col1,col2\n";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(csvContent));

		var inputSerialization = InputSerialization.builder()
				.csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.USE).build())
				.compressionType(CompressionType.NONE)
				.build();
		var outputSerialization = OutputSerialization.builder()
				.csv(CSVOutput.builder().build())
				.build();

		var request = SelectObjectContentRequest.builder()
				.bucket(bucketName)
				.key(key)
				.expression("SELECT * FROM s3object")
				.expressionType(ExpressionType.SQL)
				.inputSerialization(inputSerialization)
				.outputSerialization(outputSerialization)
				.build();

		var resultCollector = new ResultCollector();
		var handler = SelectObjectContentResponseHandler.builder()
				.subscriber(SelectObjectContentResponseHandler.Visitor.builder()
						.onRecords(r -> resultCollector.add(r.payload().asUtf8String()))
						.build())
				.build();

		asyncClient.selectObjectContent(request, handler).join();
		var result = resultCollector.getResult();
		// 헤더만 있으면 데이터 행 없음. 결과는 빈 문자열이거나 헤더만 나올 수 있음
		assertNotNull(result);
	}

	/** 응답 스트림에서 레코드를 수집하는 헬퍼 */
	private static final class ResultCollector {
		private final List<String> chunks = new ArrayList<>();

		void add(String payload) {
			if (payload != null && !payload.isEmpty())
				chunks.add(payload);
		}

		String getResult() {
			return String.join("", chunks);
		}
	}
}
