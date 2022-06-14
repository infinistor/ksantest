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
namespace ReplicationTest
{
	class MainConfig
	{
		private const string STR_DEF_FILENAME = "config.ini";
		#region Global
		private const string STR_GLOBAL = "Global";
		private const string STR_DELAY = "Delay";
		private const string STR_CHECK_VERSION_ID = "CheckVersionId";
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
		private const string STR_DB_HOST = "host";
		private const string STR_DB_PORT = "port";
		private const string STR_DB_NAME = "name";
		private const string STR_DB_USERNAME = "username";
		private const string STR_DB_PASSWORD = "password";
		#endregion

		private readonly IniFile Ini = new IniFile();

		public readonly string FileName;

		public int Delay;
		public bool CheckVersionId;
		public string NormalBucket;
		public string EncryptionBucket;
		public string TargetBucketPrefix;
		public int TestOption;
		public int SSL;

		public DBInfo DB;
		public UserData MainUser;
		public UserData AltUser;

		public BucketData Normal { get => new BucketData(){BucketName = NormalBucket, Encryption = false};}
		public BucketData Encryption { get => new BucketData(){BucketName = EncryptionBucket, Encryption = true};}

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
			NormalBucket = ReadKeyToString(STR_GLOBAL, STR_NORMAL_BUCKET_NAME);
			EncryptionBucket = ReadKeyToString(STR_GLOBAL, STR_ENCRYPTION_BUCKET_NAME);
			TargetBucketPrefix = ReadKeyToString(STR_GLOBAL, STR_TARGET_BUCKET_PREFIX);
			TestOption = ReadKeyToInt(STR_GLOBAL, STR_TEST_OPTION);
			SSL = ReadKeyToInt(STR_GLOBAL, STR_CHECK_SSL);
			DB = GetDBInfo();

			MainUser = GetUser(STR_MAINUSER);
			AltUser = GetUser(STR_ALTUSER);
		}

		private DBInfo GetDBInfo()
		=> new DBInfo()
		{
			Host = ReadKeyToString(STR_DB, STR_DB_HOST),
			Port = ReadKeyToInt(STR_DB, STR_DB_PORT),
			Name = ReadKeyToString(STR_DB, STR_DB_NAME),
			UserName = ReadKeyToString(STR_DB, STR_DB_USERNAME),
			Password = ReadKeyToString(STR_DB, STR_DB_PASSWORD)
		};

		private UserData GetUser(string Section)
		=> new UserData()
		{
			URL = ReadKeyToString(Section, STR_URL),
			Port = ReadKeyToInt(Section, STR_PORT),
			SSLPort = ReadKeyToInt(Section, STR_SSL_PORT),
			RegionName = ReadKeyToString(Section, STR_REGION_NAME),
			AccessKey = ReadKeyToString(Section, STR_ACCESSKEY),
			SecretKey = ReadKeyToString(Section, STR_SECRETKEY)
		};
		
		private string ReadKeyToString(string Section, string Key) => Ini[Section][Key].ToString();
		private int ReadKeyToInt(string Section, string Key) => int.TryParse(Ini[Section][Key].ToString(), out int Value) ? Value : -1;
		private bool ReadKeyToBoolean(string Section, string Key) => bool.TryParse(Ini[Section][Key].ToString(), out bool Value) && Value;
	}
}
