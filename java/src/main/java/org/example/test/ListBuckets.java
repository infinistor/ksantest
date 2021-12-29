package org.example.test;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;

import org.example.s3tests.MainData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.HeadBucketRequest;

public class ListBuckets extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("ListBuckets Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("ListBuckets End");
	}

	@Test
	@DisplayName("test_buckets_create_then_list")
    @Tag("Get")
    @Tag("KSAN")
    //@Tag("여러개의 버킷 생성해서 목록 조회 확인")
    public void test_buckets_create_then_list()
    {
        var Client = GetClient();
        var BucketNames = new ArrayList<String>();
        for (int i = 0; i < 5; i++)
        {
            var BucketName = GetNewBucketName();
            Client.createBucket(BucketName);
            BucketNames.add(BucketName);
        }

        var Response = Client.listBuckets();
        var BucketList = GetBucketList(Response);

        for (var BucketName : BucketNames)
        {
            if(!BucketList.contains(BucketName))
                fail(String.format("S3 implementation's GET on Service did not return bucket we created: %s", BucketName));
        }
    }

    @Test
	@DisplayName("test_list_buckets_invalid_auth")
    @Tag("ERROR")
    @Tag("KSAN")
    //@Tag("존재하지 않는 사용자가 버킷목록 조회시 에러 확인")
    public void test_list_buckets_invalid_auth()
    {
        var BadAuthClient = GetBadAuthClient(null, null);

        var e = assertThrows(AmazonServiceException.class, () -> BadAuthClient.listBuckets());
        
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(403, StatusCode);
        assertEquals(MainData.InvalidAccessKeyId, ErrorCode);
    }

    @Test
	@DisplayName("test_list_buckets_bad_auth")
    @Tag("ERROR")
    @Tag("KSAN")
    //@Tag("로그인정보를 잘못입력한 사용자가 버킷목록 조회시 에러 확인")
    public void test_list_buckets_bad_auth()
    {
        var MainAccessKey = Config.MainUser.AccessKey;
        var BadAuthClient = GetBadAuthClient(MainAccessKey, null);

        var e = assertThrows(AmazonServiceException.class, () -> BadAuthClient.listBuckets());
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(403, StatusCode);
        assertEquals(MainData.SignatureDoesNotMatch, ErrorCode);
    }
    
    @Test
    @DisplayName("test_head_bucket")
    @Tag("Metadata")
    @Tag("KSAN")
    //Tag("버킷의 메타데이터를 가져올 수 있는지 확인")
    public void test_head_bucket()
    {
    	var BucketName = GetNewBucket();
    	var Client = GetClient();
    	
    	var Response = Client.headBucket(new HeadBucketRequest(BucketName));
    	assertNotNull(Response);
    }
}
