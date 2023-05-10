package org.example.test;

import org.example.Data.MainData;
import org.example.Utility.MariaDB;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class UsedSize extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("UsedSize Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("UsedSize End");
	}

	@Test
	@Tag("PUT")
	// PutObject로 업로드할 경우 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_putobject() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "TestKey";
		var length = 123 * 456; // 56088
		var content1 = Utils.randomTextToLong(length);
		client.putObject(bucketName, key, content1);

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName);
		mdb.closePool();
	}

	@Test
	@Tag("PUT")
	// PutObject로 덮어쓸 경우 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_putobject_overwrite() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "TestKey";
		var length = 123 * 456; // 56088
		var content1 = Utils.randomTextToLong(length);
		client.putObject(bucketName, key, content1);

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName);

		length = 125 * 456; // 57000
		content1 = Utils.randomTextToLong(length);
		client.putObject(bucketName, key, content1);

		checksize(mdb, bucketName);

		mdb.closePool();
	}

	@Test
	@Tag("PUT")
	// MultipartUpload로 업로드 할 경우 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_upload() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "TestKey";
		var Size = 15 * 1048580; // 15728700
		var MetadataList = new ObjectMetadata();

		var UploadData = setupMultipartUpload(client, bucketName, key, Size, MetadataList);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, UploadData.uploadId, UploadData.parts));

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName);
		mdb.closePool();
	}

	@Test
	@Tag("PUT")
	// MultipartUpload로 덮어쓸 경우 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_upload_overwrite() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "TestKey";
		var Size = 15 * 1048580; // 15728700
		var MetadataList = new ObjectMetadata();

		var UploadData = setupMultipartUpload(client, bucketName, key, Size, MetadataList);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, UploadData.uploadId, UploadData.parts));

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName);

		Size = 17 * 1048580; // 17825860
		UploadData = setupMultipartUpload(client, bucketName, key, Size, MetadataList);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, UploadData.uploadId, UploadData.parts));

		checksize(mdb, bucketName);

		mdb.closePool();
	}

	@Test
	@Tag("PUT")
	// CopyObject로 업로드할 경우 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_copy() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Size = 15 * MainData.MB;
		var Key1 = "srcmultipart";
		var ContentType = "text/bla";

		var Key1MetaData = new ObjectMetadata();
		Key1MetaData.addUserMetadata("x-amz-meta-foo", "bar");
		Key1MetaData.setContentType(ContentType);

		var UploadData = setupMultipartUpload(client, bucketName, Key1, Size, Key1MetaData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key1, UploadData.uploadId, UploadData.parts));

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName);

		var Response = client.getObjectMetadata(bucketName, Key1);
		var VersionID = Response.getVersionId();

		var Key2 = "dstmultipart";
		client.copyObject(new CopyObjectRequest(bucketName, Key1, bucketName, Key2).withSourceVersionId(VersionID));
		Response = client.getObjectMetadata(bucketName, Key2);

		checksize(mdb, bucketName);

		mdb.closePool();
	}

	@Test
	@Tag("PUT")
	// CopyObject로 덮어쓸 경우 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_copy_overwrite() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Size = 15 * MainData.MB;
		var Key1 = "srcmultipart";
		var Key2 = "dstmultipart";
		var ContentType = "text/bla";

		var Key1MetaData = new ObjectMetadata();
		Key1MetaData.addUserMetadata("x-amz-meta-foo", "bar");
		Key1MetaData.setContentType(ContentType);

		var UploadData = setupMultipartUpload(client, bucketName, Key1, Size, Key1MetaData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key1, UploadData.uploadId, UploadData.parts));

		UploadData = setupMultipartUpload(client, bucketName, Key2, Size, Key1MetaData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key2, UploadData.uploadId, UploadData.parts));

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName);

		var Response = client.getObjectMetadata(bucketName, Key1);
		var VersionID = Response.getVersionId();

		client.copyObject(new CopyObjectRequest(bucketName, Key1, bucketName, Key2).withSourceVersionId(VersionID));
		Response = client.getObjectMetadata(bucketName, Key2);

		checksize(mdb, bucketName);

		mdb.closePool();
	}

	@Test
	@Tag("PUT")
	// 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_copyobject_diff_bucket() {
		var bucketName1 = getNewBucket();
		var bucketName2 = getNewBucket();
		var client = getClient();
		var Size = 15 * MainData.MB;
		var Key1 = "srcmultipart";
		var Key2 = "dstmultipart";
		var ContentType = "text/bla";

		var Key1MetaData = new ObjectMetadata();
		Key1MetaData.addUserMetadata("x-amz-meta-foo", "bar");
		Key1MetaData.setContentType(ContentType);

		var UploadData = setupMultipartUpload(client, bucketName1, Key1, Size, Key1MetaData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName1, Key1, UploadData.uploadId, UploadData.parts));

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName1);
		checksize(mdb, bucketName2);

		client.copyObject(new CopyObjectRequest(bucketName1, Key1, bucketName2, Key2));

		checksize(mdb, bucketName1);
		checksize(mdb, bucketName2);

		mdb.closePool();
	}

	@Test
	@Tag("PUT")
	// 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_copyobject_overwrite_diff_bucket() {
		var bucketName1 = getNewBucket();
		var bucketName2 = getNewBucket();
		var client = getClient();
		var Size = 15 * MainData.MB;
		var Key1 = "srcmultipart";
		var Key2 = "dstmultipart";
		var ContentType = "text/bla";

		var Key1MetaData = new ObjectMetadata();
		Key1MetaData.addUserMetadata("x-amz-meta-foo", "bar");
		Key1MetaData.setContentType(ContentType);

		var UploadData = setupMultipartUpload(client, bucketName1, Key1, Size, Key1MetaData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName1, Key1, UploadData.uploadId, UploadData.parts));

		UploadData = setupMultipartUpload(client, bucketName2, Key2, Size, Key1MetaData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName2, Key2, UploadData.uploadId, UploadData.parts));

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName1);
		checksize(mdb, bucketName2);

		client.copyObject(new CopyObjectRequest(bucketName1, Key1, bucketName2, Key2));

		checksize(mdb, bucketName1);
		checksize(mdb, bucketName2);

		mdb.closePool();
	}

	@Test
	@Tag("PUT")
	// 버킷의 UsedSize가 정상적으로 동작하는지 확인
	public void test_bucket_check_delete_object() {
		var bucketName1 = getNewBucket();
		var bucketName2 = getNewBucket();
		var client = getClient();
		var Size = 15 * MainData.MB;
		var Key1 = "srcmultipart";
		var Key2 = "dstmultipart";
		var ContentType = "text/bla";

		var Key1MetaData = new ObjectMetadata();
		Key1MetaData.addUserMetadata("x-amz-meta-foo", "bar");
		Key1MetaData.setContentType(ContentType);

		var UploadData = setupMultipartUpload(client, bucketName1, Key1, Size, Key1MetaData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName1, Key1, UploadData.uploadId, UploadData.parts));

		var UploadData1 = setupMultipartUpload(client, bucketName2, Key2, Size, Key1MetaData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName2, Key2, UploadData1.uploadId, UploadData1.parts));

		MariaDB mdb = MariaDB.getInstance();
		mdb.initPool();

		checksize(mdb, bucketName1);
		checksize(mdb, bucketName2);

		client.copyObject(new CopyObjectRequest(bucketName1, Key1, bucketName2, Key2));

		checksize(mdb, bucketName1);
		checksize(mdb, bucketName2);

		client.deleteObject(new DeleteObjectRequest(bucketName1, Key1));
		client.deleteObject(new DeleteObjectRequest(bucketName2, Key2));

		checksize(mdb, bucketName1);
		checksize(mdb, bucketName2);

		mdb.closePool();
		System.out.println("final step");
	}
}
