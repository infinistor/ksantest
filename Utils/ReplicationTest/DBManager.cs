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
using log4net;
using MySql.Data.MySqlClient;
using System;
using System.Reflection;

namespace ReplicationTest
{
	class DBManager(DBConfig DB, int BuildId)
	{
		private static readonly ILog _log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
		private const string DB_ID = "id";
		private const string DB_TEST_DATE = "test_date";
		private const string DB_BUILD_ID = "build_id";
		private const string DB_SOURCE_URL = "source_url";
		private const string DB_SOURCE_BUCKET = "source_bucket";
		private const string DB_SOURCE_ENCRYPTION = "source_encryption";
		private const string DB_TARGET_URL = "target_url";
		private const string DB_TARGET_BUCKET = "target_bucket";
		private const string DB_TARGET_ENCRYPTION = "target_encryption";
		private const string DB_TARGET_FILTERING = "target_filtering";
		private const string DB_TARGET_DELETEMARKER = "target_deletemarker";
		private const string DB_RESULT = "result";
		private const string DB_MESSAGE = "message";


		public readonly DBConfig DB = DB;
		public readonly int BuildId = BuildId;

		public MySqlConnection _conn = null;

		public bool Connect()
		{
			try
			{
				_conn = new MySqlConnection(DB.ConnectionString);
				_conn.Open();
				CreateTable();
				return true;
			}
			catch (Exception e)
			{
				_log.Error(e);
				return false;
			}
		}

		public void CreateTable()
		{
			if (_conn == null) return;
			var query = $"create table if not exists `{DB.TableName}` ("
						+ $"{DB_ID} int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, "
						+ $"{DB_TEST_DATE} timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "
						+ $"{DB_BUILD_ID} int(11) NOT NULL, "
						+ $"{DB_SOURCE_URL} varchar(500) NOT NULL, "
						+ $"{DB_SOURCE_BUCKET} varchar(64) NOT NULL, "
						+ $"{DB_SOURCE_ENCRYPTION} tinyint(1) NOT NULL, "
						+ $"{DB_TARGET_URL} varchar(500) NOT NULL, "
						+ $"{DB_TARGET_BUCKET} varchar(64) NOT NULL, "
						+ $"{DB_TARGET_ENCRYPTION} tinyint(1) NOT NULL, "
						+ $"{DB_TARGET_FILTERING} tinyint(1) NOT NULL, "
						+ $"{DB_TARGET_DELETEMARKER} tinyint(1) NOT NULL, "
						+ $"{DB_RESULT} varchar(256) NOT NULL, "
						+ $"{DB_MESSAGE} text"
						+ ");";
			try
			{
				MySqlCommand command = new(query, _conn);
				command.ExecuteNonQuery();
			}
			catch (Exception e)
			{
				_log.Error(query, e);
			}
		}

		public bool Insert(BucketData Source, BucketData Target, string MainUrl, string AltUrl, string Result, string Message)
		{
			if (_conn == null) return false;
			string query = $"insert into `{DB.TableName}`({DB_BUILD_ID}, {DB_SOURCE_URL}, {DB_SOURCE_BUCKET}, {DB_SOURCE_ENCRYPTION}, {DB_TARGET_URL}, {DB_TARGET_BUCKET}, {DB_TARGET_ENCRYPTION}, {DB_TARGET_FILTERING}, {DB_TARGET_DELETEMARKER}, {DB_RESULT}, {DB_MESSAGE})"
						 + $" values({BuildId}, '{MainUrl}', '{Source.BucketName}', {Source.Encryption}, '{AltUrl}', '{Target.BucketName}', {Target.Encryption}, {Target.Filtering}, {Target.DeleteMarker}, '{Result}', '{Message}');";
			try
			{

				MySqlCommand command = new(query, _conn);
				if (command.ExecuteNonQuery() != 1)
				{
					_log.ErrorFormat("Insert Failed({0})", query);
					return false;
				}
				return true;
			}
			catch (Exception e)
			{
				_log.Error(query, e);
				return false;
			}
		}

		public void Close()
		{
			_conn?.Close();
		}
	}
}
