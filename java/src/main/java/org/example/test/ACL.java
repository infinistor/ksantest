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
package org.example.test;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.util.List;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.function.Executable;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.TagSet;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class ACL extends TestBase {
	@Test
	@Tag("Access")
	public void testPrivateBucketAndObject() {
		var mainKey = "testDefaultObjectPutGetMain";
		var altKey = "testDefaultObjectPutGetAlt";
		var publicKey = "testDefaultObjectPutGetPublic";
		var bucketName = setupAclObjects(1, CannedAccessControlList.Private, CannedAccessControlList.Private, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketPublicReadObject() {
		var mainKey = "testPrivateBucketPublicObjectMain";
		var altKey = "testPrivateBucketPublicObjectAlt";
		var publicKey = "testPrivateBucketPublicObjectPublic";

		var bucketName = setupAclObjects(2, CannedAccessControlList.Private, CannedAccessControlList.PublicRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketPublicRWObject() {
		var mainKey = "testPrivateBucketPublicRWObjectMain";
		var altKey = "testPrivateBucketPublicRWObjectAlt";
		var publicKey = "testPrivateBucketPublicRWObjectPublic";

		var bucketName = setupAclObjects(3, CannedAccessControlList.Private, CannedAccessControlList.PublicReadWrite, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketAuthenticatedReadObject() {
		var mainKey = "testPrivateBucketAuthenticatedObjectMain";
		var altKey = "testPrivateBucketAuthenticatedObjectAlt";
		var publicKey = "testPrivateBucketAuthenticatedObjectPublic";

		var bucketName = setupAclObjects(4, CannedAccessControlList.Private, CannedAccessControlList.AuthenticatedRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketBucketOwnerReadObject() {
		var mainKey = "testPrivateBucketBucketOwnerReadObjectMain";
		var altKey = "testPrivateBucketBucketOwnerReadObjectAlt";
		var publicKey = "testPrivateBucketBucketOwnerReadObjectPublic";

		var bucketName = setupAclObjects(5, CannedAccessControlList.Private, CannedAccessControlList.BucketOwnerRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketBucketOwnerReadObjectUploadAltUser() {
		var mainKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserMain";
		var altKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserAlt";
		var publicKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserPublic";

		var bucketName = setupAclObjectsByAlt(6, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.BucketOwnerRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		client.setBucketAcl(bucketName, CannedAccessControlList.Private);

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(client, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		failedPutObject(altClient, bucketName, mainKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		succeedPutObject(client, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketBucketOwnerFullControlObject() {
		var mainKey = "testPrivateBucketBucketOwnerFullControlObjectMain";
		var altKey = "testPrivateBucketBucketOwnerFullControlObjectAlt";
		var publicKey = "testPrivateBucketBucketOwnerFullControlObjectPublic";

		var bucketName = setupAclObjects(7, CannedAccessControlList.Private, CannedAccessControlList.BucketOwnerFullControl, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketPrivateObject() {
		var mainKey = "testPublicReadBucketPrivateObjectMain";
		var altKey = "testPublicReadBucketPrivateObjectAlt";
		var publicKey = "testPublicReadBucketPrivateObjectPublic";

		var bucketName = setupAclObjects(8, CannedAccessControlList.PublicRead, CannedAccessControlList.Private, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketAndObject() {
		var mainKey = "testPublicReadBucketAndObjectMain";
		var altKey = "testPublicReadBucketAndObjectAlt";
		var publicKey = "testPublicReadBucketAndObjectPublic";

		var bucketName = setupAclObjects(9, CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketPublicRWObject() {
		var mainKey = "testPublicReadBucketPublicRWObjectMain";
		var altKey = "testPublicReadBucketPublicRWObjectAlt";
		var publicKey = "testPublicReadBucketPublicRWObjectPublic";

		var bucketName = setupAclObjects(10, CannedAccessControlList.PublicRead, CannedAccessControlList.PublicReadWrite, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketAuthenticatedReadObject() {
		var mainKey = "testPublicReadBucketAuthenticatedReadObjectMain";
		var altKey = "testPublicReadBucketAuthenticatedReadObjectAlt";
		var publicKey = "testPublicReadBucketAuthenticatedReadObjectPublic";

		var bucketName = setupAclObjects(11, CannedAccessControlList.PublicRead, CannedAccessControlList.AuthenticatedRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketBucketOwnerReadObject() {
		var mainKey = "testPublicReadBucketBucketOwnerReadObjectMain";
		var altKey = "testPublicReadBucketBucketOwnerReadObjectAlt";
		var publicKey = "testPublicReadBucketBucketOwnerReadObjectPublic";

		var bucketName = setupAclObjects(12, CannedAccessControlList.PublicRead, CannedAccessControlList.BucketOwnerRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketBucketOwnerFullControlObject() {
		var mainKey = "testPublicReadBucketBucketOwnerFullControlObjectMain";
		var altKey = "testPublicReadBucketBucketOwnerFullControlObjectAlt";
		var publicKey = "testPublicReadBucketBucketOwnerFullControlObjectPublic";

		var bucketName = setupAclObjects(13, CannedAccessControlList.PublicRead, CannedAccessControlList.BucketOwnerFullControl, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPrivateObject() {
		var mainKey = "testPublicRWBucketPrivateObjectMain";
		var altKey = "testPublicRWBucketPrivateObjectAlt";
		var altNewKey = "testPublicRWBucketPrivateObjectAltNew";
		var publicKey = "testPublicRWBucketPrivateObjectPublic";
		var publicNewKey = "testPublicRWBucketPrivateObjectPublicNew";

		var bucketName = setupAclObjects(14, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.Private, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPrivateObjectByAltUser() {
		var mainKey = "testPublicRWBucketPrivateObjectByAltUserMain";
		var altKey = "testPublicRWBucketPrivateObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketPrivateObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketPrivateObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(15, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.Private, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		failedGetObject(client, bucketName, mainKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);

		altClient.deleteObject(bucketName, altKey);
		altClient.deleteObject(bucketName, publicKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicReadObject() {
		var mainKey = "testPublicRWBucketPublicReadObjectMain";
		var altKey = "testPublicRWBucketPublicReadObjectAlt";
		var altNewKey = "testPublicRWBucketPublicReadObjectAltNew";
		var publicKey = "testPublicRWBucketPublicReadObjectPublic";
		var publicNewKey = "testPublicRWBucketPublicReadObjectPublicNew";

		var bucketName = setupAclObjects(16, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicReadObjectByAltUser() {
		var mainKey = "testPublicRWBucketPublicReadObjectByAltUserMain";
		var altKey = "testPublicRWBucketPublicReadObjectByAltUserAlt";
		var altNewKey = "testPublicRWBucketPublicReadObjectByAltUserAltNew";
		var publicKey = "testPublicRWBucketPublicReadObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketPublicReadObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(17, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);

		altClient.deleteObject(bucketName, altKey);
		altClient.deleteObject(bucketName, publicKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicRWObject() {
		var mainKey = "testPublicRWBucketPublicRWObjectMain";
		var altKey = "testPublicRWBucketPublicRWObjectAlt";
		var altNewKey = "testPublicRWBucketPublicRWObjectAltNew";
		var publicKey = "testPublicRWBucketPublicRWObjectPublic";
		var publicNewKey = "testPublicRWBucketPublicRWObjectPublicNew";

		var bucketName = setupAclObjects(18, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicReadWrite, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicRWObjectByAltUser() {
		var mainKey = "testPublicRWBucketPublicRWObjectByAltUserMain";
		var altKey = "testPublicRWBucketPublicRWObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketPublicRWObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketPublicRWObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(19, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicReadWrite, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketAuthenticatedReadObject() {
		var mainKey = "testPublicRWBucketAuthenticatedReadObjectMain";
		var altKey = "testPublicRWBucketAuthenticatedReadObjectAlt";
		var altNewKey = "testPublicRWBucketAuthenticatedReadObjectAltNew";
		var publicKey = "testPublicRWBucketAuthenticatedReadObjectPublic";
		var publicNewKey = "testPublicRWBucketAuthenticatedReadObjectPublicNew";

		var bucketName = setupAclObjects(20, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.AuthenticatedRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketAuthenticatedReadObjectByAltUser() {
		var mainKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserMain";
		var altKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(21, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.AuthenticatedRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerReadObject() {
		var mainKey = "testPublicRWBucketBucketOwnerReadObjectMain";
		var altKey = "testPublicRWBucketBucketOwnerReadObjectAlt";
		var altNewKey = "testPublicRWBucketBucketOwnerReadObjectAltNew";
		var publicKey = "testPublicRWBucketBucketOwnerReadObjectPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerReadObjectPublicNew";

		var bucketName = setupAclObjects(22, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.BucketOwnerRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerReadObjectByAltUser() {
		var mainKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserMain";
		var altKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(23, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.BucketOwnerRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerFullControlObject() {
		var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectMain";
		var altKey = "testPublicRWBucketBucketOwnerFullControlObjectAlt";
		var altNewKey = "testPublicRWBucketBucketOwnerFullControlObjectAltNew";
		var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectPublicNew";

		var bucketName = setupAclObjects(24, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.BucketOwnerFullControl, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerFullControlObjectByAltUser() {
		var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserMain";
		var altKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(25, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.BucketOwnerFullControl, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred() {
		var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredMain";
		var altKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAlt";
		var altNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAltNew";
		var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublicNew";

		var bucketName = setupAclObjectsByAlt(26, ObjectOwnership.BucketOwnerPreferred, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.BucketOwnerFullControl, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketPrivateObject() {
		var mainKey = "testAuthenticatedReadBucketPrivateObjectMain";
		var altKey = "testAuthenticatedReadBucketPrivateObjectAlt";
		var publicKey = "testAuthenticatedReadBucketPrivateObjectPublic";

		var bucketName = setupAclObjects(27, CannedAccessControlList.AuthenticatedRead, CannedAccessControlList.Private, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketPublicReadObject() {
		var mainKey = "testAuthenticatedReadBucketPublicReadObjectMain";
		var altKey = "testAuthenticatedReadBucketPublicReadObjectAlt";
		var publicKey = "testAuthenticatedReadBucketPublicReadObjectPublic";

		var bucketName = setupAclObjects(28, CannedAccessControlList.AuthenticatedRead, CannedAccessControlList.PublicRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketPublicRWObject() {
		var mainKey = "testAuthenticatedReadBucketPublicRWObjectMain";
		var altKey = "testAuthenticatedReadBucketPublicRWObjectAlt";
		var publicKey = "testAuthenticatedReadBucketPublicRWObjectPublic";

		var bucketName = setupAclObjects(29, CannedAccessControlList.AuthenticatedRead, CannedAccessControlList.PublicReadWrite, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketAndObject() {
		var mainKey = "testAuthenticatedReadBucketAndObjectMain";
		var altKey = "testAuthenticatedReadBucketAndObjectAlt";
		var publicKey = "testAuthenticatedReadBucketAndObjectPublic";

		var bucketName = setupAclObjects(30, CannedAccessControlList.AuthenticatedRead, CannedAccessControlList.AuthenticatedRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketBucketOwnerReadObject() {
		var mainKey = "testAuthenticatedReadBucketBucketOwnerReadObjectMain";
		var altKey = "testAuthenticatedReadBucketBucketOwnerReadObjectAlt";
		var publicKey = "testAuthenticatedReadBucketBucketOwnerReadObjectPublic";

		var bucketName = setupAclObjects(31, CannedAccessControlList.AuthenticatedRead, CannedAccessControlList.BucketOwnerRead, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketBucketOwnerFullControlObject() {
		var mainKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectMain";
		var altKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectAlt";
		var publicKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectPublic";

		var bucketName = setupAclObjects(32, CannedAccessControlList.AuthenticatedRead, CannedAccessControlList.BucketOwnerFullControl, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("List")
	public void testPrivateBucketList() {
		var keys = List.of("testPrivateBucketList1", "testPrivateBucketList2", "testPrivateBucketList3");
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = setupAclBucket(33, CannedAccessControlList.Private, keys);

		succeedListObjects(client, bucketName, keys);
		failedListObjects(altClient, bucketName, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedListObjects(publicClient, bucketName, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("List")
	public void testPublicReadBucketList() {
		var keys = List.of("testPublicReadBucketList1", "testPublicReadBucketList2", "testPublicReadBucketList3");
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = setupAclBucket(34, CannedAccessControlList.PublicRead, keys);

		succeedListObjects(client, bucketName, keys);
		succeedListObjects(altClient, bucketName, keys);
		succeedListObjects(publicClient, bucketName, keys);
	}

	@Test
	@Tag("List")
	public void testPublicRWBucketList() {
		var keys = List.of("testPublicRWBucketList1", "testPublicRWBucketList2", "testPublicRWBucketList3");
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = setupAclBucket(35, CannedAccessControlList.PublicReadWrite, keys);

		succeedListObjects(client, bucketName, keys);
		succeedListObjects(altClient, bucketName, keys);
		succeedListObjects(publicClient, bucketName, keys);
	}

	@Test
	@Tag("List")
	public void testAuthenticatedReadBucketList() {
		var keys = List.of("testAuthenticatedReadBucketList1",
				"testAuthenticatedReadBucketList2",
				"testAuthenticatedReadBucketList3");
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = setupAclBucket(36, CannedAccessControlList.AuthenticatedRead, keys);

		succeedListObjects(client, bucketName, keys);
		succeedListObjects(altClient, bucketName, keys);
		failedListObjects(publicClient, bucketName, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserFullControl() {
		var bucketName = setupBucketPermission(37, Permission.FullControl);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserRead() {
		var bucketName = setupBucketPermission(38, Permission.Read);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserReadAcp() {
		var bucketName = setupBucketPermission(39, Permission.ReadAcp);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWrite() {
		var bucketName = setupBucketPermission(40, Permission.Write);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWriteAcp() {
		var bucketName = setupBucketPermission(41, Permission.WriteAcp);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserFullControl() {
		var key = "testObjectPermissionAltUserFullControl";
		var bucketName = setupObjectPermission(42, key, Permission.FullControl);
		var altClient = getAltClient();

		checkObjectAclAllowRead(altClient, bucketName, key);
		checkObjectAclAllowReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclAllowWriteACP(altClient, bucketName, key);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserRead() {
		var key = "testObjectPermissionAltUserRead";
		var bucketName = setupObjectPermission(43, key, Permission.Read);
		var altClient = getAltClient();

		checkObjectAclAllowRead(altClient, bucketName, key);
		checkObjectAclDenyReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclDenyWriteACP(altClient, bucketName, key);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserReadAcp() {
		var key = "testObjectPermissionAltUserReadAcp";
		var bucketName = setupObjectPermission(44, key, Permission.ReadAcp);
		var altClient = getAltClient();

		checkObjectAclDenyRead(altClient, bucketName, key);
		checkObjectAclAllowReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclDenyWriteACP(altClient, bucketName, key);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserWrite() {
		var key = "testObjectPermissionAltUserWrite";
		var bucketName = setupObjectPermission(45, key, Permission.Write);
		var altClient = getAltClient();

		checkObjectAclDenyRead(altClient, bucketName, key);
		checkObjectAclDenyReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclDenyWriteACP(altClient, bucketName, key);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserWriteAcp() {
		var key = "testObjectPermissionAltUserWriteAcp";
		var bucketName = setupObjectPermission(46, key, Permission.WriteAcp);
		var altClient = getAltClient();

		checkObjectAclDenyRead(altClient, bucketName, key);
		checkObjectAclDenyReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclAllowWriteACP(altClient, bucketName, key);
	}

	// region ACL 권한 범위 (AWS 실측 기준)

	private static void assertDenied(Executable executable) {
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, executable);
	}

	/** ACL 응답을 "대상 권한" 목록으로 줄인다. 대상은 main, alt, 그룹 URI 중 하나다. */
	private List<String> grantSet(AccessControlList acl) {
		var result = new ArrayList<String>();
		for (var grant : acl.getGrantsAsList()) {
			var grantee = grant.getGrantee();
			var target = grantee.getIdentifier();
			if (!(grantee instanceof GroupGrantee)) {
				if (config.mainUser.id.equals(target))
					target = "main";
				else if (config.altUser.id.equals(target))
					target = "alt";
				else
					target = "other";
			}
			result.add(target + " " + grant.getPermission().toString());
		}
		Collections.sort(result);
		return result;
	}

	/** 비공개 버킷의 오브젝트에 서브 사용자 권한 하나만 준다. */
	private String setupObjectGrant(String key, Permission permission, int testId) {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, testId);
		client.putObject(bucketName, key, "content");
		client.setObjectAcl(bucketName, key, createAltAcl(permission));
		return bucketName;
	}

	private static PutObjectResult putObjectWithAcl(AmazonS3 client, String bucketName, String key, String content,
			CannedAccessControlList acl) {
		var body = content.getBytes(StandardCharsets.UTF_8);
		var metadata = new ObjectMetadata();
		metadata.setContentLength(body.length);
		return client.putObject(
				new PutObjectRequest(bucketName, key, new ByteArrayInputStream(body), metadata).withCannedAcl(acl));
	}

	@Test
	@Tag("Scope")
	public void testBucketAclReadScope() {
		var key = "testBucketAclReadScope";
		var client = getClient();
		var bucketName = setupBucketPermission(47, Permission.Read);
		client.putObject(bucketName, key, "content");
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "multi"))
				.getUploadId();

		var altClient = getAltClient();
		// READ 는 목록 종류를 모두 연다
		altClient.headBucket(new HeadBucketRequest(bucketName));
		assertEquals(1, altClient.listObjectsV2(bucketName).getKeyCount());
		assertEquals(1, altClient.listVersions(bucketName, "").getVersionSummaries().size());
		assertEquals(1,
				altClient.listMultipartUploads(new ListMultipartUploadsRequest(bucketName)).getMultipartUploads().size());
		// 목록 권한이 있으므로 없는 키는 404 다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY, () -> altClient.getObject(bucketName, "not-exist"));

		// 오브젝트 읽기·쓰기와 버킷 설정은 열지 않는다
		assertDenied(() -> altClient.getObject(bucketName, key));
		assertDenied(() -> altClient.putObject(bucketName, "new", "x"));
		assertDenied(() -> altClient.deleteObject(bucketName, key));
		assertDenied(() -> altClient.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "new")));
		assertDenied(() -> altClient.listParts(new ListPartsRequest(bucketName, "multi", uploadId)));
		assertDenied(() -> altClient.getBucketVersioningConfiguration(bucketName));
		assertDenied(() -> altClient.getBucketLocation(bucketName));
		// SDK v1 은 키별 오류를 예외로 돌려준다
		var e = assertThrows(MultiObjectDeleteException.class,
				() -> altClient.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(key)));
		assertTrue(e.getDeletedObjects().isEmpty());
		assertEquals(MainData.ACCESS_DENIED, e.getErrors().get(0).getCode());
	}

	@Test
	@Tag("Scope")
	public void testBucketAclWriteScope() {
		var key = "testBucketAclWriteScope";
		var client = getClient();
		var bucketName = setupBucketPermission(48, Permission.Write);
		client.putObject(bucketName, key, "content");
		client.putObject(bucketName, "other", "content");
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "multi"))
				.getUploadId();

		var altClient = getAltClient();
		// WRITE 는 쓰기·덮어쓰기·삭제·멀티파트 시작을 연다(소유자가 올린 오브젝트도)
		altClient.putObject(bucketName, "new", "x");
		altClient.putObject(bucketName, key, "overwritten");
		altClient.deleteObject(bucketName, key);
		var response = altClient.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys("other"));
		assertEquals(1, response.getDeletedObjects().size());
		assertEquals("other", response.getDeletedObjects().get(0).getKey());
		var newUploadId = altClient
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "alt-multi")).getUploadId();
		altClient.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, "alt-multi", newUploadId));
		assertEquals(List.of("new"), getKeys(client.listObjectsV2(bucketName).getObjectSummaries()));

		// 읽기·목록은 열지 않는다. 목록 권한이 없어 없는 키도 403 이다
		assertS3Error(HttpStatus.SC_FORBIDDEN, null, () -> altClient.headBucket(new HeadBucketRequest(bucketName)));
		assertDenied(() -> altClient.listObjectsV2(bucketName));
		assertDenied(() -> altClient.listMultipartUploads(new ListMultipartUploadsRequest(bucketName)));
		assertDenied(() -> altClient.getObject(bucketName, "not-exist"));
		assertDenied(() -> altClient.getBucketAcl(bucketName));
		// 다른 사용자가 시작한 멀티파트는 중단할 수 없다
		assertDenied(
				() -> altClient.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, "multi", uploadId)));
	}

	@Test
	@Tag("Scope")
	public void testBucketAclFullControlScope() {
		var client = getClient();
		var bucketName = setupBucketPermission(49, Permission.FullControl);
		var altClient = getAltClient();
		assertEquals(0, altClient.listObjectsV2(bucketName).getKeyCount());
		altClient.putObject(bucketName, "new", "x");
		altClient.deleteObject(bucketName, "new");
		assertEquals(List.of("alt FULL_CONTROL", "main FULL_CONTROL"), grantSet(altClient.getBucketAcl(bucketName)));
		altClient.setBucketAcl(bucketName, createAltAcl(Permission.FullControl));

		// ACL 의 FULL_CONTROL 은 READ·WRITE·READ_ACP·WRITE_ACP 뿐이다. 버킷 설정과 삭제는 소유자만 한다
		assertDenied(() -> altClient.getBucketVersioningConfiguration(bucketName));
		assertDenied(() -> altClient.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(
				bucketName, new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED))));
		assertDenied(() -> altClient.setBucketTaggingConfiguration(bucketName,
				new BucketTaggingConfiguration(List.of(new TagSet(Map.of("a", "b"))))));
		assertDenied(() -> altClient.getBucketPolicy(bucketName));
		assertDenied(() -> altClient.deleteBucket(bucketName));
		client.headBucket(new HeadBucketRequest(bucketName));
	}

	@Test
	@Tag("Scope")
	public void testObjectAclReadScope() {
		var key = "testObjectAclReadScope";
		var bucketName = setupObjectGrant(key, Permission.Read, 50);
		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(bucketName, key).getObjectContent()));
		assertEquals(7L, altClient.getObjectMetadata(bucketName, key).getContentLength());
		// READ 는 본문과 메타데이터만 연다 (SDK v1 에는 GetObjectAttributes 가 없어 그 확인은 뺀다)
		assertDenied(() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)));
		assertDenied(() -> altClient.getObjectAcl(bucketName, key));
		assertDenied(() -> altClient.setObjectAcl(bucketName, key, CannedAccessControlList.Private));
		assertDenied(() -> altClient.putObject(bucketName, key, "x"));
		assertDenied(() -> altClient.deleteObject(bucketName, key));
	}

	@Test
	@Tag("Scope")
	public void testObjectAclFullControlScope() {
		var key = "testObjectAclFullControlScope";
		var bucketName = setupObjectGrant(key, Permission.FullControl, 51);
		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(bucketName, key).getObjectContent()));
		assertTrue(grantSet(altClient.getObjectAcl(bucketName, key)).contains("alt FULL_CONTROL"));
		// 오브젝트의 FULL_CONTROL 에는 쓰기가 없다. 덮어쓰기·삭제는 버킷 권한으로 판정한다
		assertDenied(() -> altClient.putObject(bucketName, key, "x"));
		assertDenied(() -> altClient.deleteObject(bucketName, key));
		assertDenied(() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)));
		assertDenied(() -> altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, key,
				new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("a", "b"))))));
		// WRITE_ACP 가 있으므로 ACL 은 바꿀 수 있다
		altClient.setObjectAcl(bucketName, key, CannedAccessControlList.Private);
	}

	@Test
	@Tag("Copy")
	public void testObjectAclNotCopied() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 52);
		putObjectWithAcl(client, bucketName, "source", "content", CannedAccessControlList.PublicRead);
		assertEquals(List.of(MainData.ALL_USERS + " READ", "main FULL_CONTROL"),
				grantSet(client.getObjectAcl(bucketName, "source")));

		// 복사본은 원본의 ACL 을 물려받지 않는다
		client.copyObject(bucketName, "source", bucketName, "copy");
		assertEquals(List.of("main FULL_CONTROL"), grantSet(client.getObjectAcl(bucketName, "copy")));
		// 복사 요청에 준 ACL 이 적용된다
		client.copyObject(new CopyObjectRequest(bucketName, "source", bucketName, "canned")
				.withCannedAccessControlList(CannedAccessControlList.AuthenticatedRead));
		assertEquals(List.of(MainData.AUTHENTICATED_USERS + " READ", "main FULL_CONTROL"),
				grantSet(client.getObjectAcl(bucketName, "canned")));
		// grant 헤더만 주면 소유자 전권은 붙지 않는다
		var grantRead = new AccessControlList();
		grantRead.grantPermission(config.altUser.toGrantee(), Permission.Read);
		client.copyObject(
				new CopyObjectRequest(bucketName, "source", bucketName, "grant").withAccessControlList(grantRead));
		assertEquals(List.of("alt READ"), grantSet(client.getObjectAcl(bucketName, "grant")));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectAclResetOnOverwrite() {
		var key = "testObjectAclResetOnOverwrite";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 53);
		putObjectWithAcl(client, bucketName, key, "content", CannedAccessControlList.PublicRead);
		var publicClient = getPublicClient();
		assertEquals("content", getBody(publicClient.getObject(bucketName, key).getObjectContent()));

		// ACL 없이 덮어쓰면 비공개로 돌아간다
		client.putObject(bucketName, key, "content");
		assertEquals(List.of("main FULL_CONTROL"), grantSet(client.getObjectAcl(bucketName, key)));
		assertDenied(() -> getPublicClient().getObject(bucketName, key));
	}

	@Test
	@Tag("Multipart")
	public void testObjectAclMultipart() {
		var key = "testObjectAclMultipart";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 54);
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key)
				.withCannedACL(CannedAccessControlList.PublicRead)).getUploadId();
		var body = "content".getBytes(StandardCharsets.UTF_8);
		var partETag = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
				.withUploadId(uploadId).withPartNumber(1).withInputStream(new ByteArrayInputStream(body))
				.withPartSize(body.length)).getPartETag();
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadId, List.of(partETag)));
		// 멀티파트를 시작할 때 준 ACL 이 완성된 오브젝트의 ACL 이 된다
		assertEquals(List.of(MainData.ALL_USERS + " READ", "main FULL_CONTROL"),
				grantSet(client.getObjectAcl(bucketName, key)));
		var publicClient = getPublicClient();
		assertEquals("content", getBody(publicClient.getObject(bucketName, key).getObjectContent()));
		publicClient.getObjectMetadata(bucketName, key);
		// public-read 는 ACL 조회와 태그 조회를 열지 않는다
		assertDenied(() -> publicClient.getObjectAcl(bucketName, key));
		assertDenied(() -> publicClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)));
	}

	@Test
	@Tag("Version")
	public void testObjectAclPerVersion() {
		var key = "testObjectAclPerVersion";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 55);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var version1 = putObjectWithAcl(client, bucketName, key, "1", CannedAccessControlList.PublicRead)
				.getVersionId();
		var version2 = client.putObject(bucketName, key, "2").getVersionId();

		// ACL 은 버전마다 따로다
		assertEquals(List.of(MainData.ALL_USERS + " READ", "main FULL_CONTROL"),
				grantSet(client.getObjectAcl(bucketName, key, version1)));
		assertEquals(List.of("main FULL_CONTROL"), grantSet(client.getObjectAcl(bucketName, key, version2)));
		var publicClient = getPublicClient();
		assertEquals("1",
				getBody(publicClient.getObject(new GetObjectRequest(bucketName, key, version1)).getObjectContent()));
		assertDenied(() -> publicClient.getObject(bucketName, key));
		assertDenied(() -> publicClient.getObject(new GetObjectRequest(bucketName, key, version2)));
	}

	@Test
	@Tag("Canned")
	public void testAclCannedBucketOwner() {
		var key = "testAclCannedBucketOwner";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 56);
		putObjectWithAcl(client, bucketName, key, "content", CannedAccessControlList.PublicRead);
		var cannedAcls = List.of(CannedAccessControlList.BucketOwnerRead,
				CannedAccessControlList.BucketOwnerFullControl);
		// 버킷 소유자가 곧 오브젝트 소유자면 bucket-owner-* 는 소유자 전권만 남긴다
		for (var acl : cannedAcls) {
			client.setObjectAcl(bucketName, key, acl);
			assertEquals(List.of("main FULL_CONTROL"), grantSet(client.getObjectAcl(bucketName, key)));
		}
		// 버킷에 주면 무시되고 비공개가 된다
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
		for (var acl : cannedAcls) {
			client.setBucketAcl(bucketName, acl);
			assertEquals(List.of("main FULL_CONTROL"), grantSet(client.getBucketAcl(bucketName)));
		}
	}

	@Test
	@Tag("Scope")
	public void testPublicRwBucketAnonymousScope() {
		var key = "testPublicRwBucketAnonymousScope";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 57, CannedAccessControlList.PublicReadWrite);
		client.putObject(bucketName, key, "content");
		client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "multi"));

		var publicClient = getPublicClient();
		assertEquals(1, publicClient.listObjectsV2(bucketName).getKeyCount());
		assertEquals(1, publicClient.listVersions(bucketName, "").getVersionSummaries().size());
		assertEquals(1, publicClient.listMultipartUploads(new ListMultipartUploadsRequest(bucketName))
				.getMultipartUploads().size());
		publicClient.putObject(bucketName, "anonymous", "x");
		assertEquals("x", getBody(publicClient.getObject(bucketName, "anonymous").getObjectContent()));

		// 버킷이 열려 있어도 비공개 오브젝트와 ACL 은 열리지 않는다
		assertDenied(() -> publicClient.getObject(bucketName, key));
		assertDenied(() -> publicClient.getBucketAcl(bucketName));
		assertDenied(() -> publicClient.setBucketAcl(bucketName, CannedAccessControlList.Private));
		// 익명 사용자는 멀티파트 업로드를 시작할 수 없다
		assertDenied(
				() -> publicClient.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "new")));
	}

	// endregion
}
