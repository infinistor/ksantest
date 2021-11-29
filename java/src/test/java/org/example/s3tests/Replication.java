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
package org.example.s3tests;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Replication extends TestBase {

  @Test
  @DisplayName("test_replication_set")
  @Tag("Check")
  // @Tag("버킷의 Replication 설정이 되는지 확인(put/get/delete)")
  public void test_replication_set() {
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
    BucketReplicationConfiguration getconfig = Client.getBucketReplicationConfiguration(SourceBucketName);

    assertEquals(config.toString(), getconfig.toString());

    Client.deleteBucketReplicationConfiguration(SourceBucketName);

    var e = assertThrows(AmazonServiceException.class, () -> Client.getBucketReplicationConfiguration(SourceBucketName));
    var StatusCode = e.getStatusCode();
    assertEquals(404, StatusCode);
  }

  @Test
  @DisplayName("test_replication_no_rule")
  @Tag("Check")
  // @Tag("복제설정중 role이 없어도 설정되는지 확인")
  public void test_replication_no_rule() {
    var SourceBucketName = GetNewBucket();
    var TargetBucketName = GetNewBucket();
    var Client = GetClient();

    CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);
    CheckConfigureVersioningRetry(TargetBucketName, BucketVersioningConfiguration.ENABLED);

    String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

    ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
    ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination);
    BucketReplicationConfiguration config = new BucketReplicationConfiguration();
    config.setRoleARN(null);
    config.addRule("rule1", rule);

    Client.setBucketReplicationConfiguration(SourceBucketName, config);
    BucketReplicationConfiguration getconfig = Client.getBucketReplicationConfiguration(SourceBucketName);

    assertEquals(config.toString(), getconfig.toString());
  }

  @Test
  @DisplayName("test_replication_full_copy")
  @Tag("Check")
  // @Tag("버킷의 복제설정이 올바르게 동작하는지 확인")
  public void test_replication_full_copy() {
    var SourceBucketName = GetNewBucket();
    var TargetBucketName = GetNewBucket();
    var Client = GetClient();

    CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);
    CheckConfigureVersioningRetry(TargetBucketName, BucketVersioningConfiguration.ENABLED);

    String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

    ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
    ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination);
    BucketReplicationConfiguration config = new BucketReplicationConfiguration();
    config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
    config.addRule("rule1", rule);

    Client.setBucketReplicationConfiguration(SourceBucketName, config);
    BucketReplicationConfiguration getconfig = Client.getBucketReplicationConfiguration(SourceBucketName);

    assertEquals(config.toString(), getconfig.toString());
    Delay(5000);

    // 3개의 오브젝트 업로드
    var SourceKeys = new ArrayList<String>();
    for (int i = 0; i < 3; i++) {
      var KeyName = String.format("test%d", i);

      // 2개의 버전정보 생성
      for (int j = 0; j < 2; j++) Client.putObject(SourceBucketName, KeyName, RandomTextToLong(100));
      SourceKeys.add(KeyName);
    }

    Delay(5000);

    // 검증
    var TargetResource = Client.listObjects(TargetBucketName);
    var TargetKeys = GetKeys(TargetResource.getObjectSummaries());
    assertEquals(SourceKeys, TargetKeys);

    var SourceVersionsResource = Client.listVersions(SourceBucketName, "");
    var TargetVersionsResource = Client.listVersions(TargetBucketName, "");
    var SourceVerions = GetVersions(SourceVersionsResource.getVersionSummaries());
    var TargetVerions = GetVersions(TargetVersionsResource.getVersionSummaries());

    VersionIDsCompare(SourceVerions, TargetVerions);
  }

  @Test
  @DisplayName("test_replication_tagging")
  @Tag("Check")
  // @Tag("버킷에 복제 설정이 되어 있을때 태그가 복제되는지 확인")
  public void test_replication_tagging() {
    var SourceBucketName = GetNewBucket();
    var TargetBucketName = GetNewBucket();
    var Client = GetClient();

    CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);
    CheckConfigureVersioningRetry(TargetBucketName, BucketVersioningConfiguration.ENABLED);

    String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

    ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
    ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination);
    BucketReplicationConfiguration config = new BucketReplicationConfiguration();
    config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
    config.addRule("rule1", rule);

    Client.setBucketReplicationConfiguration(SourceBucketName, config);
    BucketReplicationConfiguration getconfig = Client.getBucketReplicationConfiguration(SourceBucketName);

    assertEquals(config.toString(), getconfig.toString());
    Delay(5000);

    // 오브젝트 업로드
    var SourceKeys = new ArrayList<String>();

    var KeyName1 = "test";
    Client.putObject(SourceBucketName, KeyName1, RandomTextToLong(100));
    SourceKeys.add(KeyName1);
    
    // 오브젝트 업로드 후 태그 추가
    var KeyName2 = "test-after-tag";
    var TagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
    var Tag1 = new com.amazonaws.services.s3.model.Tag(KeyName2, KeyName2);
    TagSets.add(Tag1);
    var InputTagSet =  new ObjectTagging(TagSets);

    Client.putObject(SourceBucketName, KeyName2, RandomTextToLong(100));
    Client.setObjectTagging(new SetObjectTaggingRequest(SourceBucketName, KeyName2, InputTagSet));
    SourceKeys.add(KeyName2);
    
    //태그를 포함한 오브젝트 업로드
    var KeyName3 = "test-together-tag";
    var Headers = new ObjectMetadata();
    Headers.setHeader("x-amz-tagging", KeyName3 + "=" + KeyName3);

    Client.putObject(SourceBucketName, KeyName3, CreateBody(RandomTextToLong(100)), Headers);
    SourceKeys.add(KeyName3);

    Delay(5000);

    // 검증
    var TargetResource = Client.listObjects(TargetBucketName);
    var TargetKeys = GetKeys(TargetResource.getObjectSummaries());
    assertEquals(SourceKeys, TargetKeys);

    var SourceVersionsResource = Client.listVersions(SourceBucketName, "");
    var TargetVersionsResource = Client.listVersions(TargetBucketName, "");
    var SourceVerions = GetVersions(SourceVersionsResource.getVersionSummaries());
    var TargetVerions = GetVersions(TargetVersionsResource.getVersionSummaries());

    VersionIDsCompare(SourceVerions, TargetVerions);

    //태그 검증
    for(String KeyName : SourceKeys)
    {
      var SourceTagResource = Client.getObjectTagging(new GetObjectTaggingRequest(SourceBucketName, KeyName));
      var TargetTagResource = Client.getObjectTagging(new GetObjectTaggingRequest(TargetBucketName, KeyName));

      TaggingCompare(SourceTagResource.getTagSet(), TargetTagResource.getTagSet());
    }
  }

  @Test
  @DisplayName("test_replication_prefix_copy")
  @Tag("Check")
  // @Tag("버킷의 복제 설정중 prefix가 올바르게 동작하는지 확인")
  public void test_replication_prefix_copy() {
    var SourceBucketName = GetNewBucket();
    var TargetBucketName = GetNewBucket();
    var Client = GetClient();
    var Prefix = "test1";

    CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);
    CheckConfigureVersioningRetry(TargetBucketName, BucketVersioningConfiguration.ENABLED);

    String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

    ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
    ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination)
                               .withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(Prefix)));
    BucketReplicationConfiguration config = new BucketReplicationConfiguration();
    config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
    config.addRule("rule1", rule);

    Client.setBucketReplicationConfiguration(SourceBucketName, config);
    BucketReplicationConfiguration getconfig = Client.getBucketReplicationConfiguration(SourceBucketName);

    assertEquals(config.toString(), getconfig.toString());

    Delay(5000);

    // 3개의 폴더 생성
    for (int i = 0; i < 3; i++) {
      var Dir = String.format("test%d", i);

      // 5개의 오브젝트 업로드
      for (int j = 0; j < 3; j++)
      {
        var KeyName = String.format("%s/%s", Dir, RandomText(10));
        Client.putObject(SourceBucketName, KeyName, RandomTextToLong(100));
      }
    }

    Delay(5000);

    // 검증
    var SourceResource = Client.listObjects(SourceBucketName, Prefix);
    var TargetResource = Client.listObjects(TargetBucketName);
    var SourceKeys = GetKeys(SourceResource.getObjectSummaries());
    var TargetKeys = GetKeys(TargetResource.getObjectSummaries());
    assertEquals(SourceKeys, TargetKeys);

    var SourceVersionsResource = Client.listVersions(SourceBucketName, Prefix);
    var TargetVersionsResource = Client.listVersions(TargetBucketName, "");
    var SourceVerions = GetVersions(SourceVersionsResource.getVersionSummaries());
    var TargetVerions = GetVersions(TargetVersionsResource.getVersionSummaries());

    VersionIDsCompare(SourceVerions, TargetVerions);
  }

  @Test
  @DisplayName("test_replication_deletemarker_copy")
  @Tag("Check")
  // @Tag("버킷의 복제 설정중 DeleteMarker가 올바르게 동작하는지 확인")
  public void test_replication_deletemarker_copy() {
    var SourceBucketName = GetNewBucket();
    var Target1BucketName = GetNewBucket("target1-");
    var Target2BucketName = GetNewBucket("target2-");
    var Client = GetClient();

    CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);
    CheckConfigureVersioningRetry(Target1BucketName, BucketVersioningConfiguration.ENABLED);
    CheckConfigureVersioningRetry(Target2BucketName, BucketVersioningConfiguration.ENABLED);

    String Target1BucketARN = "arn:aws:s3:::" + Target1BucketName;
    String Target2BucketARN = "arn:aws:s3:::" + Target2BucketName;

    ReplicationDestinationConfig destination1 = new ReplicationDestinationConfig().withBucketARN(Target1BucketARN);
    ReplicationDestinationConfig destination2 = new ReplicationDestinationConfig().withBucketARN(Target2BucketARN);
    ReplicationRule rule1 = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination1);
    ReplicationRule rule2 = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination2)
                               .withDeleteMarkerReplication(new DeleteMarkerReplication().withStatus("Enabled"));
    BucketReplicationConfiguration config = new BucketReplicationConfiguration();
    config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
    config.addRule("rule1", rule1);
    config.addRule("rule2", rule2);

    Client.setBucketReplicationConfiguration(SourceBucketName, config);
    BucketReplicationConfiguration getconfig = Client.getBucketReplicationConfiguration(SourceBucketName);


    assertEquals(config.toString(), getconfig.toString());

    var Keys = new ArrayList<String>();
    // 3개의 오브젝트 업로드
    for (int i = 0; i < 3; i++) {
      var KeyName = String.format("test%d", i);

      // 2개의 버전정보 생성
      for (int j = 0; j < 2; j++) Client.putObject(SourceBucketName, KeyName, RandomTextToLong(100));
      Keys.add(KeyName);
    }

    Delay(1000);

    //원본 삭제
    for(String KeyName : Keys)
      Client.deleteObject(SourceBucketName, KeyName);

    Delay(5000);

    // 검증
    var SourceResource = Client.listObjects(SourceBucketName);
    var Target1Resource = Client.listObjects(Target1BucketName);
    var Target2Resource = Client.listObjects(Target2BucketName);
    var SourceKeys = GetKeys(SourceResource.getObjectSummaries());
    var Target1Keys = GetKeys(Target1Resource.getObjectSummaries());
    var Target2Keys = GetKeys(Target2Resource.getObjectSummaries());
    assertEquals(0, SourceKeys.size(), Target2Keys.size());
    assertEquals(Keys, Target1Keys);

    var SourceVersionsResource = Client.listVersions(SourceBucketName, "");
    var Target1VersionsResource = Client.listVersions(Target1BucketName, "");
    var Target2VersionsResource = Client.listVersions(Target2BucketName, "");
    var SourceVerions = GetVersions(SourceVersionsResource.getVersionSummaries());
    var SourceDeleteMarkers = GetDeleteMarkers(SourceVersionsResource.getVersionSummaries());
    var Target1Verions = GetVersions(Target1VersionsResource.getVersionSummaries());
    var Target2Verions = GetVersions(Target2VersionsResource.getVersionSummaries());
    var Target2DeleteMarkers = GetDeleteMarkers(Target2VersionsResource.getVersionSummaries());

    VersionIDsCompare(SourceVerions, Target1Verions);
    VersionIDsCompare(SourceVerions, Target2Verions);
    VersionIDsCompare(SourceVerions, Target2Verions);
    VersionIDsCompare(SourceDeleteMarkers, Target2DeleteMarkers);
  }

  @Test
  @DisplayName("trest_replication_invalid_source_bucket_name")
  @Tag("ERROR")
  // @Tag("원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")
  public void trest_replication_invalid_source_bucket_name() {

    var SourceBucketName = GetNewBucketName();
    var TargetBucketName = GetNewBucketName();
    var Client = GetClient();
    var Prefix = "test1";

    String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

    ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
    ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination)
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
  @DisplayName("trest_replication_invalid_source_bucket_versioning")
  @Tag("ERROR")
  // @Tag("원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")
  public void trest_replication_invalid_source_bucket_versioning() {

    var SourceBucketName = GetNewBucket();
    var TargetBucketName = GetNewBucketName();
    var Client = GetClient();
    var Prefix = "test1";

    String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

    ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
    ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination)
                               .withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(Prefix)));
    BucketReplicationConfiguration config = new BucketReplicationConfiguration();
    config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
    config.addRule("rule1", rule);


    var e = assertThrows(AmazonS3Exception.class, () -> Client.setBucketReplicationConfiguration(SourceBucketName, config));
		int StatusCode = e.getStatusCode();
    var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals("InvalidRequest", ErrorCode);
  }
  
  @Test
  @DisplayName("trest_replication_invalid_target_bucket_name")
  @Tag("ERROR")
  // @Tag("대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")
  public void trest_replication_invalid_target_bucket_name() {

    var SourceBucketName = GetNewBucket();
    var TargetBucketName = GetNewBucketName();
    var Client = GetClient();
    var Prefix = "test1";
    
    CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);

    String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

    ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
    ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination)
                               .withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(Prefix)));
    BucketReplicationConfiguration config = new BucketReplicationConfiguration();
    config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
    config.addRule("rule1", rule);


    var e = assertThrows(AmazonS3Exception.class, () -> Client.setBucketReplicationConfiguration(SourceBucketName, config));
		int StatusCode = e.getStatusCode();
    var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals("InvalidRequest", ErrorCode);
  }
  
  @Test
  @DisplayName("trest_replication_invalid_target_bucket_versioning")
  @Tag("ERROR")
  // @Tag("대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")
  public void trest_replication_invalid_target_bucket_versioning() {

    var SourceBucketName = GetNewBucket();
    var TargetBucketName = GetNewBucket();
    var Client = GetClient();
    var Prefix = "test1";
    
    CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);

    String TargetBucketARN = "arn:aws:s3:::" + TargetBucketName;

    ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(TargetBucketARN);
    ReplicationRule rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination)
                               .withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(Prefix)));
    BucketReplicationConfiguration config = new BucketReplicationConfiguration();
    config.setRoleARN("arn:aws:iam::635518764071:role/awsreplicationtest");
    config.addRule("rule1", rule);


    var e = assertThrows(AmazonS3Exception.class, () -> Client.setBucketReplicationConfiguration(SourceBucketName, config));
		int StatusCode = e.getStatusCode();
    var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals("InvalidRequest", ErrorCode);
  }
}