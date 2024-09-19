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
package org.example.Data;

public class MainData {
	private MainData() {
	}

	public static final String MAJOR = "Major";
	public static final String MINOR = "Minor";
	public static final String EXPLANATION = "Explanation";

	public static final String RESULT = "Result";
	public static final String RESULT_SUCCESS = "Success";
	public static final String RESULT_FAILURE = "Failure";

	public static final String DIFFERENT = "Different";

	public static final String S3TESTS_INI = "S3TESTS_INI";

	public static final int KB = 1024;
	public static final int MB = 1024 * KB;
	public static final int GB = 1024 * MB;

	public static final String HTTP = "http://";
	public static final String HTTPS = "https://";

	public static final String POLICY_VERSION = "Version";
	public static final String POLICY_VERSION_DATE = "2012-10-17";
	public static final String POLICY_EFFECT = "Effect";
	public static final String POLICY_PRINCIPAL = "Principal";
	public static final String POLICY_NOT_PRINCIPAL = "NotPrincipal";
	public static final String POLICY_ACTION = "Action";
	public static final String POLICY_RESOURCE = "Resource";
	public static final String POLICY_EFFECT_ALLOW = "Allow";
	public static final String POLICY_EFFECT_DENY = "Deny";
	public static final String POLICY_STATEMENT = "Statement";
	public static final String POLICY_CONDITION = "Condition";
	public static final String POLICY_RESOURCE_PREFIX = "arn:aws:s3:::";

	// ErrorCode List 
	public static final String STATUS_CODE = "StatusCode";
	public static final String ERROR_CODE = "ErrorCode";
	public static final String INVALID_ARGUMENT = "InvalidArgument";
	public static final String INVALID_BUCKET_NAME = "InvalidBucketName";
	public static final String INVALID_ACCESS_KEY_ID = "InvalidAccessKeyId";
	public static final String BAD_REQUEST = "BadRequest";
	public static final String NO_SUCH_BUCKET = "NoSuchBucket";
	public static final String BUCKET_NOT_EMPTY = "BucketNotEmpty";
	public static final String NO_SUCH_KEY = "NoSuchKey";
	public static final String ACCESS_DENIED = "AccessDenied";
	public static final String PRECONDITION_FAILED = "PreconditionFailed";
	public static final String NOT_MODIFIED = "NotModified";
	public static final String BUCKET_ALREADY_OWNED_BY_YOU = "BucketAlreadyOwnedByYou";
	public static final String BUCKET_ALREADY_EXISTS = "BucketAlreadyExists";
	public static final String UNRESOLVABLE_GRANT_BY_EMAIL_ADDRESS = "UnresolvableGrantByEmailAddress";
	public static final String INVALID_REQUEST = "InvalidRequest";
	public static final String MALFORMED_XML = "MalformedXML";
	public static final String MALFORMED_ACL_ERROR = "MalformedACLError";
	public static final String INVALID_RANGE = "InvalidRange";
	public static final String ENTITY_TOO_SMALL = "EntityTooSmall";
	public static final String NO_SUCH_UPLOAD = "NoSuchUpload";
	public static final String INVALID_PART = "InvalidPart";
	public static final String INVALID_TAG = "InvalidTag";
	public static final String INVALID_BUCKET_STATE = "InvalidBucketState";
	public static final String INVALID_RETENTION_PERIOD = "InvalidRetentionPeriod";
	public static final String OBJECT_LOCK_CONFIGURATION_NOT_FOUND_ERROR = "ObjectLockConfigurationNotFoundError";
	public static final String NOT_IMPLEMENTED = "NotImplemented";
	public static final String SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch";
	public static final String INVALID_TARGET_BUCKET_FOR_LOGGING = "InvalidTargetBucketForLogging";
	public static final String INVALID_CONFIGURATION_ID = "InvalidConfigurationId";
	public static final String NO_SUCH_CONFIGURATION = "NoSuchConfiguration";
	public static final String NO_SUCH_CORS_CONFIGURATION = "NoSuchCORSConfiguration";
	public static final String NO_SUCH_WEBSITE_CONFIGURATION = "NoSuchWebsiteConfiguration";
	public static final String NO_SUCH_TAG_SET = "NoSuchTagSet";
	public static final String NO_SUCH_LIFECYCLE_CONFIGURATION = "NoSuchLifecycleConfiguration";
	public static final String NO_SUCH_NOTIFICATION_CONFIGURATION = "NoSuchNotificationConfiguration";
	public static final String NO_SUCH_METRICS_CONFIGURATION = "NoSuchMetricsConfiguration";
	public static final String NO_SUCH_INVENTORY_CONFIGURATION = "NoSuchInventoryConfiguration";
	public static final String NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION = "NoSuchPublicAccessBlockConfiguration";
	public static final String ACCESS_CONTROL_LIST_NOT_SUPPORTED = "AccessControlListNotSupported";

	public static final String NOT_MATCHED = "Source does not match target";

	public static final String ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers";
	public static final String AUTHENTICATED_USERS = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
}
