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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Calendar;

import org.example.s3tests.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.GetObjectRequest;

public class GetObject extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("GetObject Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("GetObject End");
	}
    
    @Test
    @Tag("ERROR")
    //@Tag("버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인")
    public void test_object_read_not_exist()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        var e = assertThrows(AmazonServiceException.class, () -> Client.getObject(BucketName, "bar"));
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();

        assertEquals(404, StatusCode);
        assertEquals(MainData.NoSuchKey, ErrorCode);
    }

    @Test
    @Tag("Ifmatch")
    //@Tag("존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인")
    public void test_get_object_ifmatch_good()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var KeyName = "foo";

        var PutResponse = Client.putObject(BucketName, KeyName, "bar");
        var ETag = PutResponse.getETag();

        var GetResponse = Client.getObject(new GetObjectRequest(BucketName, KeyName).withMatchingETagConstraint(ETag));
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals("bar", Body);
    }

    @Test
    @Tag("Ifmatch")
    //@Tag("오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인")
    public void test_get_object_ifmatch_failed()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var KeyName = "foo";

        Client.putObject(BucketName, KeyName, "bar");
        var Response = Client.getObject(new GetObjectRequest(BucketName, KeyName).withMatchingETagConstraint("ABCORZ"));
        assertNull(Response);
    }

    @Test
    @Tag("Ifnonematch")
    //@Tag("오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패")
    public void test_get_object_ifnonematch_good()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var KeyName = "foo";

        var PutResponse = Client.putObject(BucketName, KeyName, "bar");
        var ETag = PutResponse.getETag();

        var Response = Client.getObject(new GetObjectRequest(BucketName, KeyName).withNonmatchingETagConstraint(ETag));
        assertNull(Response);
    }

    @Test
    @Tag("Ifnonematch")
    //@Tag("오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공")
    public void test_get_object_ifnonematch_failed()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var KeyName = "foo";

        Client.putObject(BucketName, KeyName, "bar");

        var GetResponse = Client.getObject(new GetObjectRequest(BucketName, KeyName).withNonmatchingETagConstraint("ABCORZ"));
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals("bar", Body);
    }

    @Test
    @Tag("Ifmodifiedsince")
    //@Tag("[지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifmodifiedsince)보다 이후에 수정된 오브젝트를 조회 성공")
    public void test_get_object_ifmodifiedsince_good()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var KeyName = "foo";

        Client.putObject(BucketName, KeyName, "bar");

		var MyDay = Calendar.getInstance();
		MyDay.set(1994, 8, 29, 19, 43, 31);
        var Response = Client.getObject(new GetObjectRequest(BucketName, KeyName).withModifiedSinceConstraint(MyDay.getTime()));
        var Body = GetBody(Response.getObjectContent());
        assertEquals("bar", Body);
    }

    @Test
    @Tag("Ifmodifiedsince")
    //@Tag("[지정일을 오브젝트 업로드 시간 이후로 설정] 지정일(ifmodifiedsince)보다 이전에 수정된 오브젝트 조회 실패")
    public void test_get_object_ifmodifiedsince_failed()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var KeyName = "foo";

        Client.putObject(BucketName, KeyName, "bar");
        var Response = Client.getObject(BucketName, KeyName);
        var After = Calendar.getInstance();
        After.setTime(Response.getObjectMetadata().getLastModified());
        After.add(Calendar.SECOND, 1);

        Delay(1000);

        Response = Client.getObject(new GetObjectRequest(BucketName, KeyName).withModifiedSinceConstraint(After.getTime()));
        assertNull(Response);
    }

    @Test
    @Tag("Ifunmodifiedsince")
    //@Tag("[지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회 실패")
    public void test_get_object_ifunmodifiedsince_good()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var KeyName = "foo";

        Client.putObject(BucketName, KeyName, "bar");

		var MyDay = Calendar.getInstance();
		MyDay.set(1994, 8, 29, 19, 43, 31);
		
        var Response = Client.getObject(new GetObjectRequest(BucketName, KeyName).withUnmodifiedSinceConstraint(MyDay.getTime()));
        assertNull(Response);
    }

    @Test
    @Tag("Ifunmodifiedsince")
    //@Tag("[지정일을 오브젝트 업로드 시간 이후으로 설정] 지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회 성공")
    public void test_get_object_ifunmodifiedsince_failed()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var KeyName = "foo";

        Client.putObject(BucketName, KeyName, "bar");

		var MyDay = Calendar.getInstance();
		MyDay.set(2100, 8, 29, 19, 43, 31);
        var Response = Client.getObject(new GetObjectRequest(BucketName, KeyName).withUnmodifiedSinceConstraint(MyDay.getTime()));
        var Body = GetBody(Response.getObjectContent());
        assertEquals("bar", Body);
    }

    @Test
    @Tag("Range")
    @Tag("KSAN")
    //@Tag("지정한 범위로 오브젝트 다운로드가 가능한지 확인")
    public void test_ranged_request_response_code()
    {
        var Key = "testobj";
        var Content = "testcontent";

        var BucketName = GetNewBucket();
        var Client = GetClient();

        Client.putObject(BucketName, Key, Content);
        var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withRange(4, 7));

        var FetchedContent = GetBody(Response.getObjectContent());
        assertEquals(Content.substring(4, 8), FetchedContent);
        assertEquals("bytes 4-7/11", Response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
    }

    @Test
    @Tag("Range")
    @Tag("KSAN")
    //@Tag("지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인")
    public void test_ranged_big_request_response_code()
    {
        var Key = "testobj";
        var Content = RandomTextToLong(8 * MainData.MB);

        var BucketName = GetNewBucket();
        var Client = GetClient();

        Client.putObject(BucketName, Key, Content);
        var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withRange(3145728, 5242880));

        var FetchedContent = GetBody(Response.getObjectContent());
        assertTrue(Content.substring(3145728, 5242881).equals(FetchedContent), "Source does not match target");
        assertEquals("bytes 3145728-5242880/8388608", Response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
    }

    @Test
    @Tag("Range")
    @Tag("KSAN")
    //@Tag("특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인")
    public void test_ranged_request_skip_leading_bytes_response_code()
    {
        var Key = "testobj";
        var Content = "testcontent";

        var BucketName = GetNewBucket();
        var Client = GetClient();

        Client.putObject(BucketName, Key, Content);
        var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withRange(4));

        var FetchedContent = GetBody(Response.getObjectContent());
        assertEquals(Content.substring(4), FetchedContent);
        assertEquals("bytes 4-10/11", Response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
    }

    @Test
    @Tag("Range")
    @Tag("KSAN")
    //@Tag("끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인")
    public void test_ranged_request_return_trailing_bytes_response_code()
    {
        var Key = "testobj";
        var Content = "testcontent";

        var BucketName = GetNewBucket();
        var Client = GetClient();

        Client.putObject(BucketName, Key, Content);
        var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withRange(4));

        var FetchedContent = GetBody(Response.getObjectContent());
        assertEquals(Content.substring(Content.length() - 7), FetchedContent);
        assertEquals("bytes 4-10/11", Response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
    }

    @Test
    @Tag("Range")
    @Tag("KSAN")
    //@Tag("오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인")
    public void test_ranged_request_invalid_range()
    {
        var Key = "testobj";
        var Content = "testcontent";

        var BucketName = GetNewBucket();
        var Client = GetClient();

        Client.putObject(BucketName, Key, Content);
        var e = assertThrows(AmazonServiceException.class, () -> Client.getObject(new GetObjectRequest(BucketName, Key).withRange(40, 50)));
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(416, StatusCode);
        assertEquals(MainData.InvalidRange, ErrorCode);
    }

    @Test
    @Tag("Range")
    @Tag("KSAN")
    //@Tag("비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인")
    public void test_ranged_request_empty_object()
    {
        var Key = "testobj";
        var Content = "";

        var BucketName = GetNewBucket();
        var Client = GetClient();

        Client.putObject(BucketName, Key, Content);
        var e = assertThrows(AmazonServiceException.class, () -> Client.getObject(new GetObjectRequest(BucketName, Key).withRange(40, 50)));
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(416, StatusCode);
        assertEquals(MainData.InvalidRange, ErrorCode);
    }
    
    @Test
    @Tag("Get")
    @Tag("KSAN")
    //@Tag("같은 오브젝트를 여러번 반복하여 다운로드 성공 확인")
    public void test_get_object_many()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "foo";
        var Data = RandomTextToLong(15 * MainData.MB);

        Client.putObject(BucketName, Key, Data);
        CheckContent(BucketName, Key, Data, 50);
    }

    @Test
    @Tag("Get")
    @Tag("KSAN")
    //@Tag("같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")
    public void test_range_object_many()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "foo";
        var FileSize = 1024 * 1024 * 15;
        var Data = RandomTextToLong(FileSize);

        Client.putObject(BucketName, Key, Data);
        CheckContentUsingRandomRange(BucketName, Key, Data, 50);
    }
    
}
