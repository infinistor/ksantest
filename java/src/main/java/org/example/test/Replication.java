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

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.DeleteMarkerReplication;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.ReplicationDestinationConfig;
import com.amazonaws.services.s3.model.ReplicationRule;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.replication.ReplicationFilter;
import com.amazonaws.services.s3.model.replication.ReplicationPrefixPredicate;

import org.junit.Ignore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Replication extends TestBase
{
    @org.junit.jupiter.api.BeforeAll
    static public void BeforeAll()
    {
        System.out.println("Replication Start");
    }

    @org.junit.jupiter.api.AfterAll
    static public void AfterAll()
    {
        System.out.println("Replication End");
    }

    @Test
    @Tag("Check")
    // @Tag("버킷의 Replication 설정이 되는지 확인(put/get/delete)")
    public void test_replication_set()
    {
        var SourceBucketName = GetNewBucket();
        var TargetBucketName = GetNewBucket();
        var Client = GetClient();

        CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);
        CheckConfigureVersioningRetry(TargetBucketName, BucketVersioningConfiguration.ENABLED);

        String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

        ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
        ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination).withPrefix("");
        BucketReplicationConfiguration config = new BucketReplicationConfiguration();
        config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
        config.addRule("rule1", rule);

        Client.setBucketReplicationConfiguration(SourceBucketName, config);
        BucketReplicationConfiguration getconfig = Client.getBucketReplicationConfiguration(
                SourceBucketName);
        assertEquals(config.toString(), getconfig.toString());

        Client.deleteBucketReplicationConfiguration(SourceBucketName);

        var e = assertThrows(AmazonServiceException.class,() -> Client.getBucketReplicationConfiguration(SourceBucketName));
        var StatusCode = e.getStatusCode();
        assertEquals(404, StatusCode);
    }

    @Test
    @Tag("ERROR")
    // @Tag("원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")
    public void test_replication_invalid_source_bucket_name()
    {

        var SourceBucketName = GetNewBucketName();
        var TargetBucketName = GetNewBucketName();
        var Client = GetClient();
        var Prefix = "test1";

        String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

        ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
        ReplicationRule rule = new ReplicationRule()
                .withStatus("Enabled")
                .withDestinationConfig(destination)
                .withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(Prefix)));
        BucketReplicationConfiguration config = new BucketReplicationConfiguration();
        config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
        config.addRule("rule1", rule);

        var e = assertThrows(AmazonS3Exception.class, () -> Client.setBucketReplicationConfiguration(SourceBucketName, config));
        int StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(404, StatusCode);
        assertEquals("NoSuchBucket", ErrorCode);
    }

    @Test
    @Tag("ERROR")
    // @Tag("원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")
    public void test_replication_invalid_source_bucket_versioning()
    {

        var SourceBucketName = GetNewBucket();
        var TargetBucketName = GetNewBucketName();
        var Client = GetClient();
        var Prefix = "test1";

        String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

        ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(
                TargetBucketARN);
        ReplicationRule rule = new ReplicationRule()
                .withStatus("Enabled")
                .withDestinationConfig(destination)
                .withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(Prefix)));
        BucketReplicationConfiguration config = new BucketReplicationConfiguration();
        config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
        config.addRule("rule1", rule);

        var e = assertThrows(
                AmazonS3Exception.class,
                () -> Client.setBucketReplicationConfiguration(SourceBucketName, config));
        int StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(400, StatusCode);
        assertEquals("InvalidRequest", ErrorCode);
    }

    @Test
    @Tag("ERROR")
    // @Tag("대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")
    public void test_replication_invalid_target_bucket_name()
    {

        var SourceBucketName = GetNewBucket();
        var TargetBucketName = GetNewBucketName();
        var Client = GetClient();
        var Prefix = "test1";

        CheckConfigureVersioningRetry(
                SourceBucketName,
                BucketVersioningConfiguration.ENABLED);

        String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

        ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(
                TargetBucketARN);
        ReplicationRule rule = new ReplicationRule()
                .withStatus("Enabled")
                .withDestinationConfig(destination)
                .withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(Prefix)));
        BucketReplicationConfiguration config = new BucketReplicationConfiguration();
        config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
        config.addRule("rule1", rule);

        var e = assertThrows(
                AmazonS3Exception.class,
                () -> Client.setBucketReplicationConfiguration(SourceBucketName, config));
        int StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(400, StatusCode);
        assertEquals("InvalidRequest", ErrorCode);
    }

    @Test
    @Tag("ERROR")
    // @Tag("대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")
    public void test_replication_invalid_target_bucket_versioning()
    {

        var SourceBucketName = GetNewBucket();
        var TargetBucketName = GetNewBucket();
        var Client = GetClient();
        var Prefix = "test1";

        CheckConfigureVersioningRetry(
                SourceBucketName,
                BucketVersioningConfiguration.ENABLED);

        String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

        ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
        ReplicationRule rule = new ReplicationRule()
                .withStatus("Enabled")
                .withDestinationConfig(destination)
                .withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(Prefix)));
        BucketReplicationConfiguration config = new BucketReplicationConfiguration();
        config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
        config.addRule("rule1", rule);

        var e = assertThrows(
                AmazonS3Exception.class,
                () -> Client.setBucketReplicationConfiguration(SourceBucketName, config));
        int StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(400, StatusCode);
        assertEquals("InvalidRequest", ErrorCode);
    }
}