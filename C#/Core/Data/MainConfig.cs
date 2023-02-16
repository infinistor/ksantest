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
	public class MainConfig
	{
		#region Default Define
		// public const string STR_DEF_FILENAME = "awstests.ini";
		public const string STR_DEF_FILENAME = "s3tests_229.ini";
		public const string STR_SIGNATURE_VERSION_2 = "2";
		public const string STR_SIGNATURE_VERSION_4 = "4";
		#endregion

		#region S3 Define
		private const string STR_S3 = "S3";
		private const string STR_URL = "URL";
		private const string STR_PORT = "Port";
		private const string STR_SSL_PORT = "SSLPort";
		private const string STR_SIGNATURE_VERSION = "SignatureVersion";
		private const string STR_REGION_NAME = "RegionName";
		#endregion

		#region Fixtures Define
		private const string STR_FIXTURES = "Fixtures";
		private const string STR_SECURE = "IsSecure";
		private const string STR_KMS = "KMS";
		private const string STR_BUCKETPREFIX = "BucketPrefix";
		#endregion

		#region User Data Define
		private const string STR_MAINUSER = "Main User";
		private const string STR_ALTUSER = "Alt User";

		private const string STR_DISPLAYNAME = "DisplayName";
		private const string STR_USERID = "UserId";
		private const string STR_EMAIL = "Email";
		private const string STR_ACCESSKEY = "AccessKey";
		private const string STR_SECRETKEY = "SecretKey";
		#endregion

		#region 설정 변수 선언
		public S3Config S3;
		public string SignatureVersion;
		public string KMS;
		public bool IsSecure;

		public string BucketPrefix;

		public UserData MainUser;
		public UserData AltUser;
		#endregion

		public readonly string FileName = "";
		public readonly IniFile Ini = new IniFile();

		public MainConfig(string FileName)
		{
			Init();
			this.FileName = FileName.Trim();
		}
		public void Init()
		{
			S3 = null;
			SignatureVersion = "";
			KMS = "";
			IsSecure = false;
			BucketPrefix = string.Empty;
			MainUser = null;
			AltUser = null;
		}

		public void GetConfig()
		{
			Ini.Load(FileName);

			S3 = ReadS3Config();
			SignatureVersion = ReadKeyToString(STR_S3, STR_SIGNATURE_VERSION);
			KMS = ReadKeyToString(STR_S3, STR_KMS);
			IsSecure = ReadKeyToBoolean(STR_S3, STR_SECURE);

			BucketPrefix = ReadKeyToString(STR_FIXTURES, STR_BUCKETPREFIX);

			MainUser = ReadUser(STR_MAINUSER);
			AltUser = ReadUser(STR_ALTUSER);
		}

		#region Read Config Utility

		private S3Config ReadS3Config()
		{
			return new S3Config()
			{
				Address = ReadKeyToString(STR_S3, STR_URL),
				Port = ReadKeyToInt(STR_S3, STR_PORT),
				SSLPort = ReadKeyToInt(STR_S3, STR_SSL_PORT),
				RegionName = ReadKeyToString(STR_S3, STR_REGION_NAME),
			};
		}

		private UserData ReadUser(string Section)
		{

			return new UserData
			{
				DisplayName = ReadKeyToString(Section, STR_DISPLAYNAME),
				UserId = ReadKeyToString(Section, STR_USERID),
				Email = ReadKeyToString(Section, STR_EMAIL),
				AccessKey = ReadKeyToString(Section, STR_ACCESSKEY),
				SecretKey = ReadKeyToString(Section, STR_SECRETKEY),
			};
		}

		private string ReadKeyToString(string Section, string Key) => Ini[Section][Key].ToString();
		private int ReadKeyToInt(string Section, string Key) => int.TryParse(Ini[Section][Key].ToString(), out int Value) ? Value : -1;
		private bool ReadKeyToBoolean(string Section, string Key) => bool.TryParse(Ini[Section][Key].ToString(), out bool Value) && Value;
		#endregion
	}
}
