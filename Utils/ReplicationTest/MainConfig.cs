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
using System;
using System.Collections.Generic;
using System.Text.Json;

namespace ReplicationTest
{
	class MainConfig
	{
		private const string STR_DEF_FILENAME = "config.ini";
		#region Global
		private const string STR_GLOBAL = "Global";
		private const string STR_DELAY = "Delay";
		private const string STR_CHECK_VERSION_ID = "CheckVersionId";
		private const string STR_CHECK_ETAG = "CheckEtag";
		private const string STR_NORMAL_BUCKET_NAME = "NormalBucket";
		private const string STR_ENCRYPTION_BUCKET_NAME = "EncryptionBucket";
		private const string STR_TARGET_BUCKET_PREFIX = "TargetBucketPrefix";
		private const string STR_TEST_OPTION = "TestOption";
		private const string STR_CHECK_SSL = "SSL";
		#endregion
		#region  User Data
		private const string STR_MAINUSER = "Main User";
		private const string STR_ALTUSER = "Alt User";
		private const string STR_URL = "URL";
		private const string STR_PORT = "Port";
		private const string STR_SSL_PORT = "SSLPort";
		private const string STR_REGION_NAME = "RegionName";
		private const string STR_ACCESSKEY = "AccessKey";
		private const string STR_SECRETKEY = "SecretKey";
		#endregion
		#region DB
		private const string STR_DB = "DB";
		private const string STR_DB_HOST = "Host";
		private const string STR_DB_PORT = "Port";
		private const string STR_DB_NAME = "DBName";
		private const string STR_DB_TABLE_NAME = "TableName";
		private const string STR_DB_USERNAME = "UserName";
		private const string STR_DB_PASSWORD = "Password";
		#endregion

		private readonly IniFile Ini = [];

		public readonly string FileName;

		public int Delay { get; private set; }
		public bool CheckVersionId { get; private set; }
		public bool CheckEtag { get; private set; }
		public string NormalBucket { get; private set; }
		public string EncryptionBucket { get; private set; }
		public string TargetBucketPrefix { get; private set; }
		public int TestOption { get; private set; }
		public int SSL { get; private set; }

		public BucketData Normal { get; private set; }
		public BucketData Encryption { get; private set; }

		public DBConfig DB { get; private set; }
		public UserData MainUser { get; private set; }
		public UserData AltUser { get; private set; }

		public MainConfig(string FileName = null)
		{
			if (FileName == null) this.FileName = STR_DEF_FILENAME;
			else this.FileName = FileName;
		}

		public void GetConfig()
		{
			Ini.Load(FileName);

			Delay = ReadKeyToInt(STR_GLOBAL, STR_DELAY);
			CheckVersionId = ReadKeyToBoolean(STR_GLOBAL, STR_CHECK_VERSION_ID);
			CheckEtag = ReadKeyToBoolean(STR_GLOBAL, STR_CHECK_ETAG);
			NormalBucket = ReadKeyToString(STR_GLOBAL, STR_NORMAL_BUCKET_NAME);
			EncryptionBucket = ReadKeyToString(STR_GLOBAL, STR_ENCRYPTION_BUCKET_NAME);
			TargetBucketPrefix = ReadKeyToString(STR_GLOBAL, STR_TARGET_BUCKET_PREFIX);
			TestOption = ReadKeyToInt(STR_GLOBAL, STR_TEST_OPTION);
			SSL = ReadKeyToInt(STR_GLOBAL, STR_CHECK_SSL);
			DB = GetDBConfig();

			MainUser = GetUser(STR_MAINUSER);
			AltUser = GetUser(STR_ALTUSER);

			Normal = new() { BucketName = NormalBucket, Encryption = false };
			Encryption = new() { BucketName = EncryptionBucket, Encryption = true };

			// 설정 검증
			ValidateConfig();
		}

		private void ValidateConfig()
		{
			var errors = new List<string>();

			// Global 설정 검증
			if (Delay <= 0) errors.Add($"{STR_GLOBAL}.{STR_DELAY} must be greater than 0 (current: {Delay})");
			if (string.IsNullOrWhiteSpace(NormalBucket)) errors.Add($"{STR_GLOBAL}.{STR_NORMAL_BUCKET_NAME} is required");
			if (string.IsNullOrWhiteSpace(EncryptionBucket)) errors.Add($"{STR_GLOBAL}.{STR_ENCRYPTION_BUCKET_NAME} is required");
			if (string.IsNullOrWhiteSpace(TargetBucketPrefix)) errors.Add($"{STR_GLOBAL}.{STR_TARGET_BUCKET_PREFIX} is required");
			if (TestOption < 0 || TestOption > 2) errors.Add($"{STR_GLOBAL}.{STR_TEST_OPTION} must be 0, 1, or 2 (current: {TestOption})");
			if (SSL < 0 || SSL > 2) errors.Add($"{STR_GLOBAL}.{STR_CHECK_SSL} must be 0, 1, or 2 (current: {SSL})");

			// DB 설정 검증
			if (DB != null)
			{
				if (string.IsNullOrWhiteSpace(DB.Host)) errors.Add($"{STR_DB}.{STR_DB_HOST} is required");
				if (DB.Port <= 0 || DB.Port > 65535) errors.Add($"{STR_DB}.{STR_DB_PORT} must be between 1 and 65535 (current: {DB.Port})");
				if (string.IsNullOrWhiteSpace(DB.DBName)) errors.Add($"{STR_DB}.{STR_DB_NAME} is required");
				if (string.IsNullOrWhiteSpace(DB.TableName)) errors.Add($"{STR_DB}.{STR_DB_TABLE_NAME} is required");
				if (string.IsNullOrWhiteSpace(DB.UserName)) errors.Add($"{STR_DB}.{STR_DB_USERNAME} is required");
			}

			// Main User 설정 검증
			if (MainUser != null)
			{
				if (string.IsNullOrWhiteSpace(MainUser.URL)) errors.Add($"{STR_MAINUSER}.{STR_URL} is required");
				if (MainUser.Port <= 0 || MainUser.Port > 65535) errors.Add($"{STR_MAINUSER}.{STR_PORT} must be between 1 and 65535 (current: {MainUser.Port})");
				if (MainUser.SSLPort <= 0 || MainUser.SSLPort > 65535) errors.Add($"{STR_MAINUSER}.{STR_SSL_PORT} must be between 1 and 65535 (current: {MainUser.SSLPort})");
				if (string.IsNullOrWhiteSpace(MainUser.AccessKey)) errors.Add($"{STR_MAINUSER}.{STR_ACCESSKEY} is required");
				if (string.IsNullOrWhiteSpace(MainUser.SecretKey)) errors.Add($"{STR_MAINUSER}.{STR_SECRETKEY} is required");
			}

			// Alt User 설정 검증 (TestOption이 LOCAL_ONLY가 아닐 때만)
			if (TestOption != 1 && AltUser != null)
			{
				if (string.IsNullOrWhiteSpace(AltUser.URL)) errors.Add($"{STR_ALTUSER}.{STR_URL} is required");
				if (AltUser.Port <= 0 || AltUser.Port > 65535) errors.Add($"{STR_ALTUSER}.{STR_PORT} must be between 1 and 65535 (current: {AltUser.Port})");
				if (AltUser.SSLPort <= 0 || AltUser.SSLPort > 65535) errors.Add($"{STR_ALTUSER}.{STR_SSL_PORT} must be between 1 and 65535 (current: {AltUser.SSLPort})");
				if (string.IsNullOrWhiteSpace(AltUser.AccessKey)) errors.Add($"{STR_ALTUSER}.{STR_ACCESSKEY} is required");
				if (string.IsNullOrWhiteSpace(AltUser.SecretKey)) errors.Add($"{STR_ALTUSER}.{STR_SECRETKEY} is required");
			}

			if (errors.Count > 0)
			{
				throw new InvalidOperationException($"Configuration validation failed:\n{string.Join("\n", errors)}");
			}
		}

		private DBConfig GetDBConfig()
		=> new
		(
			ReadKeyToString(STR_DB, STR_DB_HOST),
			ReadKeyToInt(STR_DB, STR_DB_PORT),
			ReadKeyToString(STR_DB, STR_DB_NAME),
			ReadKeyToString(STR_DB, STR_DB_TABLE_NAME),
			ReadKeyToString(STR_DB, STR_DB_USERNAME),
			ReadKeyToString(STR_DB, STR_DB_PASSWORD)
		);

		private UserData GetUser(string Section)
		=> new()
		{
			URL = ReadKeyToString(Section, STR_URL),
			Port = ReadKeyToInt(Section, STR_PORT),
			SSLPort = ReadKeyToInt(Section, STR_SSL_PORT),
			RegionName = ReadKeyToString(Section, STR_REGION_NAME),
			AccessKey = ReadKeyToString(Section, STR_ACCESSKEY),
			SecretKey = ReadKeyToString(Section, STR_SECRETKEY)
		};

		private string ReadKeyToString(string Section, string Key)
		{
			if (!Ini.ContainsSection(Section) || !Ini[Section].ContainsKey(Key))
				return string.Empty;
			return Ini[Section][Key].ToString();
		}

		private int ReadKeyToInt(string Section, string Key)
		{
			if (!Ini.ContainsSection(Section) || !Ini[Section].ContainsKey(Key))
				return -1;
			return int.TryParse(Ini[Section][Key].ToString(), out int Value) ? Value : -1;
		}

		private bool ReadKeyToBoolean(string Section, string Key)
		{
			if (!Ini.ContainsSection(Section) || !Ini[Section].ContainsKey(Key))
				return false;
			return bool.TryParse(Ini[Section][Key].ToString(), out bool Value) && Value;
		}

		public override string ToString()
		{
			// json 형식으로 변환
			return JsonSerializer.Serialize(this);
		}
	}
}
