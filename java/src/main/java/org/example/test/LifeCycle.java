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
package org.example.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import org.example.s3tests.MainData;
import org.junit.Ignore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AbortIncompleteMultipartUpload;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.NoncurrentVersionExpiration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleTagPredicate;

public class LifeCycle extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("LifeCycle Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("LifeCycle End");
	}

	@Test
	@DisplayName("test_lifecycle_set")
	@Tag("Check")
	@Tag("KSAN")
	//@Tag("버킷의 Lifecycle 규칙을 추가 가능한지 확인")
	public void test_lifecycle_set() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		Rules.add(new Rule().withId("rule2").withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.DISABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);

		Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
	}

	@Test
	@DisplayName("test_lifecycle_get")
	@Tag("Get")
	@Tag("KSAN")
	//@Tag("버킷에 설정한 Lifecycle 규칙을 가져올 수 있는지 확인")
	public void test_lifecycle_get() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpirationInDays(31)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		Rules.add(new Rule().withId("rule2").withExpirationInDays(120)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);

		Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
		var Response = Client.getBucketLifecycleConfiguration(BucketName);
		PrefixLifecycleConfigurationCheck(Rules, Response.getRules());
	}

	@Test
	@DisplayName("test_lifecycle_get_no_id")
	@Tag("Check")
	//@Tag("ID 없이 버킷에 Lifecycle 규칙을 설정 할 수 있는지 확인")
	public void test_lifecycle_get_no_id() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withExpirationInDays(31)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		Rules.add(new Rule().withExpirationInDays(120)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);

		Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
		var Response = Client.getBucketLifecycleConfiguration(BucketName);
		var CurrentLifeCycle = Response.getRules();

		for (int i = 0; i < Rules.size(); i++) {
			assertNotNull(CurrentLifeCycle.get(i).getId());
			assertEquals(Rules.get(i).getExpirationDate(), CurrentLifeCycle.get(i).getExpirationDate());
			assertEquals(Rules.get(i).getExpirationInDays(), CurrentLifeCycle.get(i).getExpirationInDays());
			assertEquals(((LifecyclePrefixPredicate) Rules.get(i).getFilter().getPredicate()).getPrefix(),
					((LifecyclePrefixPredicate) CurrentLifeCycle.get(i).getFilter().getPredicate()).getPrefix());
			assertEquals(Rules.get(i).getStatus(), CurrentLifeCycle.get(i).getStatus());
		}
	}

	@Test
	@DisplayName("test_lifecycle_expiration_versioning_enabled")
    @Tag("Version")
    //@Tag("버킷에 버저닝 설정이 되어있는 상태에서 Lifecycle 규칙을 추가 가능한지 확인")
    public void test_lifecycle_expiration_versioning_enabled()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "test1/a";
        CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
        CreateMultipleVersions(Client, BucketName, Key, 1, true);
        Client.deleteObject(BucketName, Key);

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("expire1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
		
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);

        var Response = Client.listVersions(BucketName, null);
        var Versions = GetVersions(Response.getVersionSummaries());
        var DeleteMarkers = GetDeleteMarkers(Response.getVersionSummaries());
        assertEquals(1, Versions.size());
        assertEquals(1, DeleteMarkers.size());
    }

	@Test
	@DisplayName("test_lifecycle_id_too_long")
    @Tag("Check")
    //@Tag("버킷에 Lifecycle 규칙을 설정할때 ID의 길이가 너무 길면 실패하는지 확인")
    public void test_lifecycle_id_too_long()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId(RandomTextToLong(256)).withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("tset1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        var e = assertThrows(AmazonServiceException.class, () -> Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle));
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(400, StatusCode);
        assertEquals(MainData.InvalidArgument, ErrorCode);
    }

	@Test
	@DisplayName("test_lifecycle_same_id")
    @Tag("Duplicate")
    //@Tag("버킷에 Lifecycle 규칙을 설정할때 같은 ID로 규칙을 여러개 설정할경우 실패하는지 확인")
    public void test_lifecycle_same_id()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("tset1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		Rules.add(new Rule().withId("rule1").withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("tset2/")))
				.withStatus(BucketLifecycleConfiguration.DISABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        var e = assertThrows(AmazonServiceException.class, () -> Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle));
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(400, StatusCode);
        if(StringUtils.isBlank(Config.URL)) assertEquals(MainData.InvalidArgument, ErrorCode);
    }

	@Test
	@DisplayName("test_lifecycle_invalid_status")
    @Tag("ERROR")
    //@Tag("버킷에 Lifecycle 규칙중 status를 잘못 설정할때 실패하는지 확인")
    public void test_lifecycle_invalid_status()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("tset1/")))
				.withStatus("invalid"));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        var e = assertThrows(AmazonServiceException.class, () -> Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle));
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(400, StatusCode);
        assertEquals(MainData.MalformedXML, ErrorCode);
    }

	@Test
	@DisplayName("test_lifecycle_set_date")
    @Tag("Date")
    //@Tag("버킷의 Lifecycle규칙에 날짜를 입력가능한지 확인")
    public void test_lifecycle_set_date()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpirationDate(new Calendar.Builder().setDate(2099, 10, 10).setTimeZone(TimeZone.getTimeZone("GMT")).build().getTime())
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("tset1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
    }

	@Test
	@DisplayName("test_lifecycle_set_invalid_date")
    @Tag("ERROR")
    //@Tag("버킷의 Lifecycle규칙에 날짜를 올바르지 않은 형식으로 입력했을때 실패 확인")
    public void test_lifecycle_set_invalid_date()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpirationDate(new Calendar.Builder().setDate(2099, 10, 10).build().getTime())
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("tset1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        var e = assertThrows(AmazonServiceException.class, () -> Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle));
        var StatusCode = e.getStatusCode();
        assertEquals(400, StatusCode);
    }

	@Test
	@DisplayName("test_lifecycle_set_noncurrent")
    @Tag("Version")
    //@Tag("버킷의 버저닝설정이 없는 환경에서 버전관리용 Lifecycle이 올바르게 설정되는지 확인")
    public void test_lifecycle_set_noncurrent()
    {
        var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "past/foo", "future/bar" })));
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(2))
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("past/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		Rules.add(new Rule().withId("rule2").withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(3))
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("futrue/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
    }

	@Test
	@DisplayName("test_lifecycle_noncur_expiration")
    @Tag("Version")
    //@Tag("버킷의 버저닝설정이 되어있는 환경에서 Lifecycle 이 올바르게 동작하는지 확인")
    public void test_lifecycle_noncur_expiration()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        
        CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
        CreateMultipleVersions(Client, BucketName, "test1/a", 3, true);
        CreateMultipleVersions(Client, BucketName, "test2/abc", 3, false);

        var Response = Client.listVersions(BucketName, null);
        var InitVersions = Response.getVersionSummaries();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(2))
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
		Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
        
        assertEquals(6, InitVersions.size());
    }

	@Test
	@DisplayName("test_lifecycle_set_deletemarker")
    @Tag("DeleteMarker")
    //@Tag("DeleteMarker에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인")
    public void test_lifecycle_set_deletemarker()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpiredObjectDeleteMarker(true)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
        
    }

	@Test
	@DisplayName("test_lifecycle_set_filter")
    @Tag("Filter")
    //@Tag("Lifecycle 규칙에 필터링값을 설정 할 수 있는지 확인")
    public void test_lifecycle_set_filter()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpiredObjectDeleteMarker(true)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("foo")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
        
    }

	@Test
	@DisplayName("test_lifecycle_set_empty_filter")
    @Tag("Filter")
    //@Tag("Lifecycle 규칙에 필터링에 비어있는 값을 설정 할 수 있는지 확인")
    public void test_lifecycle_set_empty_filter()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpiredObjectDeleteMarker(true)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
        
    }

	@Test
	@DisplayName("test_lifecycle_deletemarker_expiration")
    @Tag("DeleteMarker")
    //@Tag("DeleteMarker에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인")
    public void test_lifecycle_deletemarker_expiration()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
        CreateMultipleVersions(Client, BucketName, "test1/a", 1, true);
        CreateMultipleVersions(Client, BucketName, "test2/abc", 1, false);
        Client.deleteObject(BucketName, "test1/a");
        Client.deleteObject(BucketName, "test2/abc");

        var Response = Client.listVersions(BucketName, null);
        var TotalVersions = Response.getVersionSummaries();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(1))
				.withExpiredObjectDeleteMarker(true)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
        
        assertEquals(4, TotalVersions.size());
    }

	@Test
	@DisplayName("test_lifecycle_set_multipart")
    @Tag("Multipart")
    //@Tag("AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인")
    public void test_lifecycle_set_multipart()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1")
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED)
				.withAbortIncompleteMultipartUpload(new AbortIncompleteMultipartUpload().withDaysAfterInitiation(2)));
		Rules.add(new Rule().withId("rule2")
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED)
				.withAbortIncompleteMultipartUpload(new AbortIncompleteMultipartUpload().withDaysAfterInitiation(3)));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
        
    }

	@Test
	@DisplayName("test_lifecycle_multipart_expiration")
    @Tag("Multipart")
    //@Tag("AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인")
    public void test_lifecycle_multipart_expiration()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "test1/a", "test2/b" }));
        var UploadIDs = new ArrayList<String>();

        for (var Key : KeyNames)
        {
            var Response = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key));
            UploadIDs.add(Response.getUploadId());
        }

        var ListResponse = Client.listMultipartUploads(new ListMultipartUploadsRequest(BucketName));
        var InitUploads = ListResponse.getMultipartUploads();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1")
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED)
				.withAbortIncompleteMultipartUpload(new AbortIncompleteMultipartUpload().withDaysAfterInitiation(2)));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
        Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
        assertEquals(2, InitUploads.size());
    }
	
	@Test
	@DisplayName("test_lifecycle_delete")
	@Tag("Delete")
	@Tag("KSAN")
	// @Tag("버킷의 Lifecycle 규칙을 삭제 가능한지 확인")
	public void test_lifecycle_delete()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		Rules.add(new Rule().withId("rule2").withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.DISABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);

		Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
		Client.deleteBucketLifecycleConfiguration(BucketName);
	}

	@Test
	@Ignore // 테스트 규격이 확정되지 않음
	@DisplayName("test_lifecycle_set_and")
	@Tag("Get")
	@Tag("KSAN")
	//@Tag("버킷에 다양한 Lifecycle 설정이 가능한지 확인")
	public void test_lifecycle_set_and() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
        CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId("rule1")
				.withExpirationInDays(31) // 31일 뒤에 삭제
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/"))) // Object명이 test1/ 으로 시작할 경우에만 동작
				.withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(31)) // 오브젝트의 최신버전을 제외한 나머지 버전일 경우 31일 뒤에 삭제
				.withAbortIncompleteMultipartUpload(new AbortIncompleteMultipartUpload().withDaysAfterInitiation(31)) // Multipart의 유예시간을 31일로 설정
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		Rules.add(new Rule().withId("rule2")
				.withExpiredObjectDeleteMarker(true) // Object의 모든 버전이 삭제되고 DeleteMarker만 남았을 경우 삭제
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/"))) // Object명이 test2/ 으로 시작할 경우에만 동작
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		Rules.add(new Rule().withId("rule3").withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(31))// 오브젝트의 최신버전을 제외한 나머지 버전일 경우 31일 뒤에 삭제
				.withFilter(new LifecycleFilter(new LifecycleTagPredicate(new com.amazonaws.services.s3.model.Tag("Filter", "001"))))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);

		Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);
		var Response = Client.getBucketLifecycleConfiguration(BucketName);
		PrefixLifecycleConfigurationCheck(Rules, Response.getRules());
	}

}
