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
		/******************************** Default ********************************/
		private const string STR_DEFAULT = "Default";
		private const string STR_DELAY = "Delay";
		/****************************** S3 Setting *******************************/
		private const string STR_MAINUSER = "Main User";
		private const string STR_ALTUSER = "Alt User";
		private const string STR_URL = "URL";
		private const string STR_ACCESSKEY = "AccessKey";
		private const string STR_SECRETKEY = "SecretKey";
		/***************************** DB Setting ********************************/
		private const string STR_DB = "DB";
		private const string STR_DB_HOST = "host";
		private const string STR_DB_PORT = "port";
		private const string STR_DB_NAME = "name";
		private const string STR_DB_USERNAME = "username";
		private const string STR_DB_PASSWORD = "password";

		private readonly IniFile Ini = new IniFile();

		public readonly string FileName;

		public int Delay;
		public DBInfo DB;
		public UserData MainUser;
		public UserData AltUser;

		public MainConfig(string FileName = null)
		{
			if (FileName == null) this.FileName = STR_DEF_FILENAME;
			else this.FileName = FileName;
		}

		public void GetConfig()
		{
			Ini.Load(FileName);

			Delay = ReadKeyToInt(STR_DEFAULT, STR_DELAY);

			DB = GetDBInfo();

			MainUser = GetUser(STR_MAINUSER);
			AltUser = GetUser(STR_ALTUSER);
		}

		private DBInfo GetDBInfo()
		{
			string Host = ReadKeyToString(STR_DB, STR_DB_HOST);
			int Port = ReadKeyToInt(STR_DB, STR_DB_PORT);
			string Name = ReadKeyToString(STR_DB, STR_DB_NAME);
			string UserName = ReadKeyToString(STR_DB, STR_DB_USERNAME);
			string Password = ReadKeyToString(STR_DB, STR_DB_PASSWORD);

			return new DBInfo(Host, Port, Name, UserName, Password);
		}

		private UserData GetUser(string Section)
		{
			string URL = ReadKeyToString(Section, STR_URL);
			string AccessKey = ReadKeyToString(Section, STR_ACCESSKEY);
			string SecretKey = ReadKeyToString(Section, STR_SECRETKEY);

			return new UserData()
			{
				URL = URL,
				AccessKey = AccessKey,
				SecretKey = SecretKey
			};
		}

		private string ReadKeyToString(string Section, string Key) => Ini[Section][Key].ToString();
		private int ReadKeyToInt(string Section, string Key) => int.TryParse(Ini[Section][Key].ToString(), out int Value) ? Value : -1;
		private bool ReadKeyToBoolean(string Section, string Key) => bool.TryParse(Ini[Section][Key].ToString(), out bool Value) && Value;
	}
}
