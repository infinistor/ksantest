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
namespace s3tests
{
	public class MainData
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

		//ErrorCode List
		public const string StatusCode = "StatusCode";
		public const string ErrorCode = "ErrorCode";
		public const string InvalidArgument = "InvalidArgument";
		public const string InvalidBucketName = "InvalidBucketName";
		public const string InvalidAccessKeyId = "InvalidAccessKeyId";
		public const string BadRequest = "BadRequest";
		public const string NoSuchBucket = "NoSuchBucket";
		public const string BucketNotEmpty = "BucketNotEmpty";
		public const string NoSuchKey = "NoSuchKey";
		public const string AccessDenied = "AccessDenied";
		public const string PreconditionFailed = "PreconditionFailed";
		public const string NotModified = "NotModified";
		public const string BucketAlreadyOwnedByYou = "BucketAlreadyOwnedByYou";
		public const string BucketAlreadyExists = "BucketAlreadyExists";
		public const string UnresolvableGrantByEmailAddress = "UnresolvableGrantByEmailAddress";
		public const string InvalidRequest = "InvalidRequest";
		public const string MalformedXML = "MalformedXML";
		public const string InvalidRange = "InvalidRange";
		public const string EntityTooSmall = "EntityTooSmall";
		public const string NoSuchUpload = "NoSuchUpload";
		public const string InvalidPart = "InvalidPart";
		public const string InvalidTag = "InvalidTag";
		public const string InvalidBucketState = "InvalidBucketState";
		public const string InvalidRetentionPeriod = "InvalidRetentionPeriod";
		public const string ObjectLockConfigurationNotFoundError = "ObjectLockConfigurationNotFoundError";
		public const string NotImplemented = "NotImplemented";
		public const string SignatureDoesNotMatch = "SignatureDoesNotMatch";
		public const string PermanentRedirect = "PermanentRedirect";
		public const string NoSuchPublicAccessBlockConfiguration = "NoSuchPublicAccessBlockConfiguration";

	}
}
