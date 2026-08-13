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

		public const string STR_DEF_FILENAME = "config.ini";
		#endregion

		#region S3 Define
		private const string STR_S3 = "S3";
		private const string STR_URL = "URL";
		private const string STR_PORT = "Port";
		private const string STR_SSL_PORT = "SSLPort";
		private const string STR_SIGNATURE_VERSION = "SignatureVersion";
		private const string STR_REGION_NAME = "RegionName";
		private const string STR_SECURE = "IsSecure";
		#endregion

		#region Fixtures Define

		private const string STR_FIXTURES = "Fixtures";
		private const string STR_BUCKET_PREFIX = "BucketPrefix";
		private const string STR_BUCKET_DELETE = "NotDelete";

		#endregion

		#region User Data Define
		private const string STR_MAIN_USER = "Main User";
		private const string STR_ALT_USER = "Alt User";
		private const string STR_BACKEND_USER = "Backend User";

		private const string STR_DISPLAY_NAME = "DisplayName";
		private const string STR_USER_ID = "UserID";
		private const string STR_EMAIL = "Email";
		private const string STR_ACCESS_KEY = "AccessKey";
		private const string STR_SECRET_KEY = "SecretKey";
		private const string STR_KMS = "KMS";
		private const string STR_X_AUTH_TOKEN = "XAuthToken";
		#endregion

		#region 설정 변수 선언
		public S3Config S3 { get; private set; }
		public string SignatureVersion { get; private set; }
		public bool IsSecure { get; private set; }
		public string BucketPrefix { get; private set; }
		public bool NotDelete { get; private set; }

		public UserData MainUser { get; private set; }
		public UserData AltUser { get; private set; }
		public UserData BackendUser { get; private set; }
		#endregion

		readonly string FileName;
		readonly IniFile Ini = [];

		public MainConfig(string FileName)
		{
			Init();
			this.FileName = FileName.Trim();
		}
		public void Init()
		{
			S3 = null;
			SignatureVersion = "";
			IsSecure = false;
			BucketPrefix = string.Empty;
			NotDelete = false;
			MainUser = null;
			AltUser = null;
			BackendUser = null;
		}

		public void GetConfig()
		{
			Ini.Load(FileName);

			S3 = ReadS3Config();
			SignatureVersion = ReadKeyToString(STR_S3, STR_SIGNATURE_VERSION);
			// Java는 [S3]에서 읽지만, 공유 11.151.ini / sample.ini는 [Fixtures]에 둔다. 둘 다 허용.
			IsSecure = HasKey(STR_FIXTURES, STR_SECURE)
				? ReadKeyToBoolean(STR_FIXTURES, STR_SECURE)
				: ReadKeyToBoolean(STR_S3, STR_SECURE);

			BucketPrefix = ReadKeyToString(STR_FIXTURES, STR_BUCKET_PREFIX);
			NotDelete = ReadKeyToBoolean(STR_FIXTURES, STR_BUCKET_DELETE);

			MainUser = ReadUser(STR_MAIN_USER);
			AltUser = ReadUser(STR_ALT_USER);
			BackendUser = ReadUserSafe(STR_BACKEND_USER);
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

		private UserData ReadUser(string Section) => new()
		{
			DisplayName = ReadKeyToString(Section, STR_DISPLAY_NAME),
			UserId = ReadKeyToString(Section, STR_USER_ID),
			Email = ReadKeyToString(Section, STR_EMAIL),
			AccessKey = ReadKeyToString(Section, STR_ACCESS_KEY),
			SecretKey = ReadKeyToString(Section, STR_SECRET_KEY),
			Kms = ReadKeyToString(Section, STR_KMS),
			XAuthToken = ReadKeyToString(Section, STR_X_AUTH_TOKEN),
		};

		/// <summary>섹션이 없을 수 있는 사용자(Backend User 등)를 관대하게 읽는다. 누락시 null 반환.</summary>
		private UserData ReadUserSafe(string Section)
		{
			try
			{
				var user = ReadUser(Section);
				if (string.IsNullOrEmpty(user.AccessKey)) return null;
				return user;
			}
			catch { return null; }
		}

		private bool HasKey(string Section, string Key) => Ini.ContainsSection(Section) && Ini[Section].ContainsKey(Key);
		private string ReadKeyToString(string Section, string Key) => Ini[Section][Key].ToString();
		private int ReadKeyToInt(string Section, string Key) => int.TryParse(Ini[Section][Key].ToString(), out int Value) ? Value : -1;
		private bool ReadKeyToBoolean(string Section, string Key) => bool.TryParse(Ini[Section][Key].ToString(), out bool Value) && Value;
		#endregion
	}
}
