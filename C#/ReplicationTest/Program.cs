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
using log4net.Config;
using System.Reflection;

[assembly: XmlConfigurator(ConfigFile = "LogConfig.xml")]

namespace ReplicationTest
{
	class Program
	{
		private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
		static void Main(string[] args)
		{
			DBManager DB = null;
			int BuildID = 0;
			string ConfigPath = null;

			// Config 설정
			if (args.Length > 1)
			{
				ConfigPath = args[1];
				if (string.IsNullOrEmpty(ConfigPath))
				{
					log.Error("config path is empty!");
					return;
				}
			}

			MainConfig Config = new MainConfig(ConfigPath);
			Config.GetConfig();
			log.Info("Get Config!");

			// DB 설정
			if (args.Length > 0)
			{
				if (!int.TryParse(args[0], out BuildID))
				{
					log.Error("is Not Build id");
					return;
				}
				if (BuildID > 0)
				{
					//DB 연결
					DB = new DBManager(Config.DB, BuildID);
					if (!DB.Connect())
					{
						log.Error("DB is not connected");
						return;
					}
					log.Info("DB is connected!");
				}
			}

			var Test = new ReplicationTest(Config, DB);
			Test.Test();
		}
	}
}
