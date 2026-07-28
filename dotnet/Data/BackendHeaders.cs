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
	public class BackendHeaders
	{
		public const string S3_NOT_ACTIVATED = "This S3 is not active.";

		public const string HEADER_DATA = "NONE";

		public const string IFS_ADMIN = "x-ifs-admin";
		public const string IFS_BACKEND = "x-ifs-backend";
		public const string IFS_VERSION_ID = "x-ifs-version-id";
		public const string DELETE_MARKER_VERSION_ID = "x-ifs-delete-marker-version-id";

		public const string HEADER_REPLICATION = "x-ifs-replication";
		public const string HEADER_LOGGING = "x-ifs-logging";
		public const string HEADER_LIFECYCLE = "x-ifs-lifecycle";
		public const string HEADER_INVENTORY = "x-ifs-inventory";

		public const string HEADER_USER_AGENT = "User-Agent";
		public const string HEADER_USER_AGENT_VALUE = "s3tests/1.1.0";
		public const string HEADER_USER_AGENT_REPLICATION = HEADER_USER_AGENT_VALUE + " replication";
		public const string HEADER_USER_AGENT_LOGGING = HEADER_USER_AGENT_VALUE + " logging";
		public const string HEADER_USER_AGENT_LIFECYCLE = HEADER_USER_AGENT_VALUE + " lifecycle";
		public const string HEADER_USER_AGENT_INVENTORY = HEADER_USER_AGENT_VALUE + " inventory";
		public const string HEADER_USER_AGENT_NOTIFICATION = HEADER_USER_AGENT_VALUE + " notification";

		public const string HEADER_REPLICATION_STATUS = "x-amz-replication-status";
		public const string HEADER_REPLICATION_STATUS_COMPLETED = "COMPLETED";
		public const string HEADER_REPLICATION_STATUS_PENDING = "PENDING";
		public const string HEADER_REPLICATION_STATUS_FAILED = "FAILED";
		public const string HEADER_REPLICATION_STATUS_REPLICA = "REPLICA";

		public const string S3_VALUE_VERSION_ID = "?versionId=";
		public const string S3_NULL_VERSION = "null";

		public const string KSAN_BACKEND = "x-ksan-backend";
		public const string KSAN_VERSION_ID = "x-ksan-version-id";
		public const string KSAN_DELETE_MARKER_VERSION_ID = "x-ksan-delete-marker-version-id";
		public const string KSAN_REPLICATION = "x-ksan-replication";
	}
}
