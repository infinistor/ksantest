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
namespace s3tests
{
	static public class MainData
	{
		public const string Major = "Major";
		public const string Minor = "Minor";
		public const string Explanation = "Explanation";

		public const string Result = "Result";
		public const string ResultSuccess = "Success";
		public const string ResultFailure = "Failure";


		public const string Different = "Different";

		public const string True = "True";
		public const string False = "False";

		public const string S3TESTS_INI = "S3TESTS_INI";

		public const int KB = 1024;
		public const int MB = 1024 * 1024;

		public const string HTTP = "http://";
		public const string HTTPS = "https://";

		public const string PolicyVersion = "Version";
		public const string PolicyVersionDate = "2012-10-17";
		public const string PolicyEffect = "Effect";
		public const string PolicyPrincipal = "Principal";
		public const string PolicyNotPrincipal = "NotPrincipal";
		public const string PolicyAction = "Action";
		public const string PolicyResource = "Resource";
		public const string PolicyEffectAllow = "Allow";
		public const string PolicyEffectDeny = "Deny";
		public const string PolicyStatement = "Statement";
		public const string PolicyCondition = "Condition";
		public const string PolicyResourcePrefix = "arn:aws:s3:::";

		#region ErrorCode
		public const string INVALID_ARGUMENT = "InvalidArgument";
		public const string INVALID_BUCKET_NAME = "InvalidBucketName";
		public const string INVALID_ACCESS_KEY_ID = "InvalidAccessKeyId";
		public const string BAD_REQUEST = "BadRequest";
		public const string NO_SUCH_BUCKET = "NoSuchBucket";
		public const string BUCKET_NOT_EMPTY = "BucketNotEmpty";
		public const string NO_SUCH_KEY = "NoSuchKey";
		public const string ACCESS_DENIED = "AccessDenied";
		public const string PRECONDITION_FAILED = "PreconditionFailed";
		public const string NOT_MODIFIED = "NotModified";
		public const string BUCKET_ALREADY_OWNED_BY_YOU = "BucketAlreadyOwnedByYou";
		public const string BUCKET_ALREADY_EXISTS = "BucketAlreadyExists";
		public const string UNRESOLVABLE_GRANT_BY_EMAIL_ADDRESS = "UnresolvableGrantByEmailAddress";
		public const string INVALID_REQUEST = "InvalidRequest";
		public const string MALFORMED_XML = "MalformedXML";
		public const string INVALID_RANGE = "InvalidRange";
		public const string ENTITY_TOO_SMALL = "EntityTooSmall";
		public const string NO_SUCH_UPLOAD = "NoSuchUpload";
		public const string INVALID_PART = "InvalidPart";
		public const string INVALID_TAG = "InvalidTag";
		public const string INVALID_BUCKET_STATE = "InvalidBucketState";
		public const string INVALID_RETENTION_PERIOD = "InvalidRetentionPeriod";
		public const string OBJECT_LOCK_CONFIGURATION_NOT_FOUND_ERROR = "ObjectLockConfigurationNotFoundError";
		public const string NOT_IMPLEMENTED = "NotImplemented";
		public const string SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch";
		public const string PERMANENT_REDIRECT = "PermanentRedirect";
		public const string NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION = "NoSuchPublicAccessBlockConfiguration";
		public const string AUTHORIZATION_QUERY_PARAMETERS_ERROR = "AuthorizationQueryParametersError";
		public const string INVALID_TARGET_BUCKET_FOR_LOGGING = "InvalidTargetBucketForLogging";
		public const string NO_SUCH_CONFIGURATION = "NoSuchConfiguration";
		public const string INVALID_CONFIGURATION_ID = "InvalidConfigurationId";
		#endregion
	}
}
