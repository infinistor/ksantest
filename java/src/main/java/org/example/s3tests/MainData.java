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

public class MainData
{
    public static final String Major = "Major";
    public static final String Minor = "Minor";
    public static final String Explanation = "Explanation";
 
    public static final String Result = "Result";
    public static final String ResultSuccess = "Success";
    public static final String ResultFailure = "Failure";
    
    
    public static final String Different = "Different";

    public static final String True = "True";
    public static final String False = "False";

    public static final String S3TESTS_INI = "S3TESTS_INI";

    public static final int KB = 1024;
    public static final int MB = 1024 * KB;
    public static final int GB = 1024 * MB;

    public static final String HTTP = "http://";
    public static final String HTTPS = "https://";

    public static final String PolicyVersion = "Version";
    public static final String PolicyVersionDate = "2012-10-17";
    public static final String PolicyEffect = "Effect"; 
    public static final String PolicyPrincipal = "Principal";
    public static final String PolicyNotPrincipal = "NotPrincipal";
    public static final String PolicyAction = "Action";
    public static final String PolicyResource = "Resource";
    public static final String PolicyEffectAllow = "Allow";
    public static final String PolicyEffectDeny = "Deny";
    public static final String PolicyStatement = "Statement";
    public static final String PolicyCondition = "Condition";
    public static final String PolicyResourcePrefix = "arn:aws:s3:::";

    //ErrorCode List
    public static final String StatusCode = "StatusCode";
    public static final String ErrorCode = "ErrorCode";
    public static final String InvalidArgument = "InvalidArgument";
    public static final String InvalidBucketName = "InvalidBucketName";
    public static final String InvalidAccessKeyId = "InvalidAccessKeyId";
    public static final String BadRequest = "BadRequest";
    public static final String NoSuchBucket = "NoSuchBucket";
    public static final String BucketNotEmpty = "BucketNotEmpty";
    public static final String NoSuchKey = "NoSuchKey";
    public static final String AccessDenied = "AccessDenied";
    public static final String PreconditionFailed = "PreconditionFailed";
    public static final String NotModified = "NotModified";
    public static final String BucketAlreadyOwnedByYou = "BucketAlreadyOwnedByYou";
    public static final String BucketAlreadyExists = "BucketAlreadyExists";
    public static final String UnresolvableGrantByEmailAddress = "UnresolvableGrantByEmailAddress";
    public static final String InvalidRequest = "InvalidRequest";
    public static final String MalformedXML = "MalformedXML";
    public static final String InvalidRange = "InvalidRange";
    public static final String EntityTooSmall = "EntityTooSmall";
    public static final String NoSuchUpload = "NoSuchUpload";
    public static final String InvalidPart = "InvalidPart";
    public static final String InvalidTag = "InvalidTag";
    public static final String InvalidBucketState = "InvalidBucketState";
    public static final String InvalidRetentionPeriod = "InvalidRetentionPeriod";
    public static final String ObjectLockConfigurationNotFoundError = "ObjectLockConfigurationNotFoundError";
    public static final String NotImplemented = "NotImplemented";
    public static final String SignatureDoesNotMatch = "SignatureDoesNotMatch";
}
