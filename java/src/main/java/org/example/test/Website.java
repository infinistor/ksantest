package org.example.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;

import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;

public class Website extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Website Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Website End");
	}

    @Test
	@DisplayName("test_webiste_get_buckets")
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("버킷의 Websize 설정 조회 확인")
    public void test_webiste_get_buckets()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        var Response = Client.getBucketWebsiteConfiguration(BucketName);
        assertNull(Response);
    }

    @Test
	@DisplayName("test_webiste_put_buckets")
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("버킷의 Websize 설정이 가능한지 확인")
    public void test_webiste_put_buckets()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        var WebConfig = new BucketWebsiteConfiguration();
        WebConfig.setErrorDocument("400");
        WebConfig.setIndexDocumentSuffix("a");

        Client.setBucketWebsiteConfiguration(BucketName, WebConfig);

        var GetResponse = Client.getBucketWebsiteConfiguration(BucketName);
        assertEquals(WebConfig.getErrorDocument(), GetResponse.getErrorDocument());
    }

    @Test
	@DisplayName("test_webiste_delete_buckets")
    @Tag("Delete")
    @Tag("KSAN")
    //@Tag("버킷의 Websize 설정이 삭제가능한지 확인")
    public void test_webiste_delete_buckets()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        var WebConfig = new BucketWebsiteConfiguration();
        WebConfig.setErrorDocument("400");
        WebConfig.setIndexDocumentSuffix("a");

        Client.setBucketWebsiteConfiguration(BucketName, WebConfig);
        Client.deleteBucketWebsiteConfiguration(BucketName);
    }
}
